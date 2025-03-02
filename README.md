# DistributedServerlessCompute
Distributed, Fault-Tolerant, Serverless Computing for Java Programs

## Table of Contents
1. [What is it?](#what-is-it)
2. [Features](#features)
    1. [HTTP Gateway](#http-gateway)
    2. [Leader Election](#leader-election)
    3. [Java Compute](#java-compute)
    4. [Fault Tolerance](#fault-tolerance)
3. [How to use](#how-to-use)
    1. [Start Cluster](#start-cluster)
    2. [Send Code to Compute](#send-code-to-compute)

## What is it?

A distributed computing system for running Java code in the cloud inspired by AWS Lambda. 

## Features

### HTTP Gateway
Gateway accepts HTTP requests from the network. Keeps cache of responses and sends request to cluster leader if not cached.

### Leader Election
Uses leader election algorithm similar to Apache Zookeeper, the leader is chosen based on the ID of the server, the highest value wins.   
 
### Java Compute
Follower servers accept jobs from the leader via a round-robin assigning algorithm and run the code and send the results back to the leader who send it out via the HTTP gateway.  

### Fault Tolerance
Uses a fail-fast fault system, once a server is unresponsive, assume dead. When a server thinks the leader died, it enters the election state. 

## How to use

### Start Cluster
Start up a number of PeerServers and one Gateway server, and wait for servers to come online and elect a leader - will show in logs. 

### Send Code to Compute 
Send HTTP requests to the address of the Gateway and get a response :). 
