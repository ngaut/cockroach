// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

// Run using: go run local_cluster.go

package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/fsouza/go-dockerclient"
)

const (
	dockerspyImage = "iverberk/docker-spy"
	domain         = "local"
)

var cockroachImage = flag.String("i", "cockroachdb/cockroach-dev", "the docker image to run")
var cockroachEntry = flag.String("e", "", "the entry point for the image")
var numNodes = flag.Int("n", 3, "the number of nodes to start")
var checkGossip = flag.Bool("c", false, "check gossip peerings and exit")

func prettyJSON(v interface{}) string {
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(pretty)
}

// Construct a new docker client using the best available method. On
// darwin (Mac OS X), first look for the DOCKER_HOST env variable. If
// not found, run "boot2docker shellinit" and add the exported
// variables to the environment. If DOCKER_HOST is set, initialize the
// client using DOCKER_TLS_VERIFY and DOCKER_CERT_PATH. If DOCKER_HOST
// is not set, look for the unix domain socket in /run/docker.sock and
// /var/run/docker.sock.
func newDockerClient() *docker.Client {
	if runtime.GOOS == "darwin" && os.Getenv("DOCKER_HOST") == "" {
		output, err := exec.Command("boot2docker", "shellinit").Output()
		if err != nil {
			log.Fatal(err)
		}
		exportRE := regexp.MustCompile(`export (\w+)=([^\n]+)`)
		for s := bufio.NewScanner(bytes.NewReader(output)); s.Scan(); {
			export := exportRE.FindStringSubmatch(s.Text())
			if len(export) != 3 {
				continue
			}
			if err := os.Setenv(export[1], export[2]); err != nil {
				log.Fatal(err)
			}
		}
	}

	host := os.Getenv("DOCKER_HOST")
	if host != "" {
		if os.Getenv("DOCKER_TLS_VERIFY") == "" {
			c, err := docker.NewClient(host)
			if err != nil {
				log.Fatal(err)
			}
			return c
		}
		certPath := os.Getenv("DOCKER_CERT_PATH")
		c, err := docker.NewTLSClient(
			host, certPath+"/cert.pem", certPath+"/key.pem", certPath+"/ca.pem")
		if err != nil {
			log.Fatal(err)
		}
		return c
	}

	for _, l := range []string{"/run/docker.sock", "/var/run/docker.sock"} {
		if _, err := os.Stat(l); err == nil {
			continue
		}
		c, err := docker.NewClient("unix://" + l)
		if err != nil {
			return nil
		}
		return c
	}
	log.Fatal("docker not configured")
	return nil
}

// Retrieve the IP address of docker itself.
func dockerIP() net.IP {
	host := os.Getenv("DOCKER_HOST")
	if host != "" {
		u, err := url.Parse(host)
		if err != nil {
			panic(err)
		}
		h, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			panic(err)
		}
		return net.ParseIP(h)
	}
	if runtime.GOOS == "linux" {
		return net.IPv4zero
	}
	panic(fmt.Errorf("unable to determine docker ip address"))
}

type container struct {
	*docker.Container
	client *docker.Client
}

// createContainer creates a new container using the specified options. Per the
// docker API, the created container is not running and must be started
// explicitly.
func createContainer(client *docker.Client, opts docker.CreateContainerOptions) (*container, error) {
	c, err := client.CreateContainer(opts)
	if err != nil {
		return nil, err
	}
	return &container{c, client}, nil
}

// remove removes the container from docker. It is an error to remove a running
// container.
func (c *container) remove() *container {
	err := c.client.RemoveContainer(docker.RemoveContainerOptions{
		ID:            c.ID,
		RemoveVolumes: true,
	})
	if err != nil {
		panic(err)
	}
	return c
}

// kill stops a running container and removes it.
func (c *container) kill() *container {
	err := c.client.KillContainer(docker.KillContainerOptions{
		ID: c.ID,
	})
	if err != nil {
		panic(err)
	}
	c.remove()
	return c
}

// start starts a non-running container.
//
// TODO(pmattis): Generalize the setting of parameters here.
func (c *container) start(binds []string, dns *container) *container {
	var dnsAddrs []string
	if dns != nil {
		dnsAddrs = append(dnsAddrs, dns.NetworkSettings.IPAddress)
	}
	err := c.client.StartContainer(c.ID, &docker.HostConfig{
		Binds:           binds,
		PublishAllPorts: true,
		DNS:             dnsAddrs,
	})
	if err != nil {
		panic(err)
	}
	return c
}

// wait waits for a running container to exit.
func (c *container) wait() *container {
	code, err := c.client.WaitContainer(c.ID)
	if err != nil {
		panic(err)
	}
	if code != 0 {
		c.logs()
		panic(fmt.Errorf("non-zero exit code: %d", code))
	}
	return c
}

// logs outputs the containers logs to stdout/stderr.
func (c *container) logs() *container {
	err := c.client.Logs(docker.LogsOptions{
		Container:    c.ID,
		OutputStream: os.Stdout,
		ErrorStream:  os.Stderr,
		Stdout:       true,
		Stderr:       true,
	})
	if err != nil {
		panic(err)
	}
	return c
}

// inspect retrieves detailed info about a container.
func (c *container) inspect() *container {
	out, err := c.client.InspectContainer(c.ID)
	if err != nil {
		panic(err)
	}
	c.Container = out
	return c
}

// addr returns the address to connect to the specified port.
//
// TODO(pmattis): Allow the desired port to be specified.
func (c *container) addr() *net.TCPAddr {
	mappings := c.NetworkSettings.PortMappingAPI()
	return &net.TCPAddr{
		IP:   dockerIP(),
		Port: int(mappings[0].PublicPort),
	}
}

type localCluster struct {
	numNodes int
	client   *docker.Client
	dns      *container
	vols     *container
	nodes    []*container
	sig      chan os.Signal
}

func createLocalCluster(client *docker.Client, numNodes int) (l *localCluster) {
	l = &localCluster{
		numNodes: numNodes,
		client:   client,
		sig:      make(chan os.Signal, 1),
	}
	defer l.stopOnPanic()
	signal.Notify(l.sig, os.Interrupt)
	l.init()
	return l
}

func (l *localCluster) stopOnPanic() {
	if r := recover(); r != nil {
		l.stop()
		if r != l {
			panic(r)
		}
	}
}

func (l *localCluster) panicOnSig() {
	if l.sig == nil {
		panic(l)
	}

	select {
	case <-l.sig:
		l.sig = nil
		panic(l)
	default:
	}
}

func (l *localCluster) runDockerSpy() {
	l.panicOnSig()

	create := func() (*container, error) {
		return createContainer(l.client, docker.CreateContainerOptions{
			Name: "docker-spy",
			Config: &docker.Config{
				Image: dockerspyImage,
				Cmd:   []string{"--dns-domain=" + domain},
			},
		})
	}
	c, err := create()
	if err == docker.ErrNoSuchImage {
		err = l.client.PullImage(docker.PullImageOptions{
			Repository:   dockerspyImage,
			Tag:          "latest",
			OutputStream: os.Stdout,
		}, docker.AuthConfiguration{})
		c, err = create()
	}
	if err != nil {
		panic(err)
	}
	c.start([]string{"/var/run/docker.sock:/var/run/docker.sock"}, nil)
	c = c.inspect()
	c.Name = "docker-spy"
	l.dns = c
	log.Printf("started %s: %s\n", c.Name, c.NetworkSettings.IPAddress)
}

// create the volumes container that keeps all of the volumes used by the
// cluster.
func (l *localCluster) createVolumes() {
	l.panicOnSig()

	vols := map[string]struct{}{}
	for i := 0; i < l.numNodes; i++ {
		vols[l.data(i)] = struct{}{}
	}
	c, err := createContainer(l.client, docker.CreateContainerOptions{
		Config: &docker.Config{
			Image:   *cockroachImage,
			Volumes: vols,
		},
	})
	if err != nil {
		panic(err)
	}
	c.start([]string{os.ExpandEnv("${PWD}/certs:/certs")}, nil)
	c.Name = "volumes"
	log.Printf("created volumes")
	l.vols = c
}

func (l *localCluster) createRoach(i int, cmd ...string) *container {
	l.panicOnSig()

	var hostname string
	if i >= 0 {
		hostname = fmt.Sprintf("roach%d", i)
	}
	var entrypoint []string
	if *cockroachEntry != "" {
		entrypoint = append(entrypoint, *cockroachEntry)
	}
	c, err := createContainer(l.client, docker.CreateContainerOptions{
		Config: &docker.Config{
			Hostname:     hostname,
			Domainname:   domain,
			Image:        *cockroachImage,
			VolumesFrom:  l.vols.ID,
			ExposedPorts: map[docker.Port]struct{}{"8080/tcp": {}},
			Entrypoint:   entrypoint,
			Cmd:          cmd,
		}})
	if err != nil {
		panic(err)
	}
	return c
}

func (l *localCluster) createCACert() {
	log.Printf("creating ca")
	c := l.createRoach(-1, "cert", "--certs=/certs", "create-ca")
	defer c.remove()
	c.start(nil, nil)
	c.wait()
}

func (l *localCluster) createNodeCerts() {
	log.Printf("creating node certs: ./certs")
	var nodes []string
	for i := 0; i < l.numNodes; i++ {
		nodes = append(nodes, l.node(i))
	}
	args := []string{"cert", "--certs=/certs", "create-node"}
	args = append(args, nodes...)
	c := l.createRoach(-1, args...)
	defer c.remove()
	c.start(nil, nil)
	c.wait()
}

func (l *localCluster) initCluster() {
	log.Printf("initializing cluster")
	c := l.createRoach(-1, "init", "--stores=ssd="+l.data(0))
	defer c.remove()
	c.start(nil, nil)
	c.wait()
}

func (l *localCluster) init() {
	l.runDockerSpy()
	l.createVolumes()
	l.createCACert()
	l.createNodeCerts()
	l.initCluster()
}

func (l *localCluster) startNode(i int) *container {
	cmd := []string{
		"start",
		"--stores=ssd=" + l.data(i),
		"--certs=/certs",
		"--addr=" + l.node(i) + ":8080",
		"--gossip=" + l.node(0) + ":8080",
	}
	c := l.createRoach(i, cmd...)
	c.start(nil, l.dns)
	c = c.inspect()
	c.Name = l.node(i)
	ports := c.NetworkSettings.PortMappingAPI()
	log.Printf("started %s: %s:%d", c.Name, dockerIP(), ports[0].PublicPort)
	return c
}

func (l *localCluster) start() bool {
	defer l.stopOnPanic()
	for i := 0; i < *numNodes; i++ {
		l.nodes = append(l.nodes, l.startNode(i))
	}
	return true
}

func (l *localCluster) stop() {
	if l.dns != nil {
		l.dns.kill()
		l.dns = nil
	}
	if l.vols != nil {
		l.vols.kill()
		l.vols = nil
		os.RemoveAll("certs")
	}
	for _, n := range l.nodes {
		n.kill()
	}
	l.nodes = nil
}

func (l *localCluster) node(i int) string {
	return fmt.Sprintf("roach%d.%s", i, domain)
}

func (l *localCluster) data(i int) string {
	return fmt.Sprintf("/data%d", i)
}

func checkGossipNodes(client *http.Client, node *container) int {
	resp, err := client.Get("https://" + node.addr().String() + "/_status/gossip")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return 0
	}
	count := 0
	infos := m["infos"].(map[string]interface{})
	for k := range infos {
		if strings.HasPrefix(k, "node:") {
			count++
		}
	}
	return count
}

func checkGossipPeerings(l *localCluster, attempts int, done chan bool) {
	go func() {
		ok := false
		defer func() {
			done <- ok
		}()

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			}}

		log.Printf("waiting for complete gossip network of %d peerings",
			len(l.nodes)*len(l.nodes))

		for i := 0; i < attempts; i++ {
			time.Sleep(1 * time.Second)
			found := 0
			for j := 0; j < len(l.nodes); j++ {
				found += checkGossipNodes(client, l.nodes[j])
			}
			fmt.Printf("%d ", found)
			if found == len(l.nodes)*len(l.nodes) {
				fmt.Printf("... all nodes verified in the cluster\n")
				ok = true
				return
			}
		}

		fmt.Printf("... failed to verify all nodes in cluster\n")
	}()
}

func main() {
	log.SetFlags(log.Ltime)
	flag.Parse()

	client := newDockerClient()
	cluster := createLocalCluster(client, *numNodes)
	if !cluster.start() {
		return
	}
	defer cluster.stop()

	var done chan bool
	if *checkGossip {
		done = make(chan bool, 1)
		checkGossipPeerings(cluster, 20, done)
	} else {
		log.Printf("cluster ready")
	}

	select {
	case <-cluster.sig:
	case <-done:
	}
}
