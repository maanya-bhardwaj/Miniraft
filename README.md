# MiniRAFT – Distributed Consensus & Fault-Tolerant Replication System

## Overview

MiniRAFT is a distributed systems project that demonstrates the implementation of the **RAFT Consensus Algorithm** using a multi-node replicated architecture. The project simulates how distributed replicas maintain consistency, elect leaders, and replicate data reliably even in the presence of failures.

The system is built using a microservice-style architecture with multiple replicas, a gateway service, and a frontend interface orchestrated through Docker.

This project showcases core distributed systems concepts such as:

* Consensus Algorithms
* Leader Election
* Log Replication
* Fault Tolerance
* Distributed State Management
* Containerized Distributed Deployment

---

# Project Goals

The objective of MiniRAFT is to model how modern distributed databases and fault-tolerant systems maintain consistency across multiple nodes.

The project demonstrates:

* Communication between distributed replicas
* Replicated state synchronization
* Leader-based consensus handling
* Distributed request coordination
* Failure-resilient architecture
* Scalable containerized deployment

---

# System Architecture

The system consists of multiple independent services:

```bash
MiniRAFT/
│
├── frontend/        # User interface layer
├── gateway/         # Request routing and coordination service
├── replica1/        # Replica node 1
├── replica2/        # Replica node 2
├── replica3/        # Replica node 3
├── docker-compose.yml
├── MiniRAFT.pdf
└── MiniRAFT_documentation.docx
```

---

# Core Features

## Distributed Replica Nodes

The project includes multiple replica nodes that simulate distributed servers participating in consensus.

### Achievements

* Distributed state replication
* Independent node execution
* Inter-node coordination
* Simulated fault tolerance
* Replicated data consistency

---

## RAFT Consensus Simulation

The project models the behavior of the RAFT protocol used in distributed systems.

### Achievements

* Leader-based coordination
* Consensus-driven updates
* Replica synchronization
* Distributed agreement handling
* Reliable state propagation

---

## Gateway Service

The gateway acts as the communication layer between clients and distributed replicas.

### Achievements

* Request routing
* Distributed coordination
* Centralized API handling
* Node communication management
* Service orchestration

---

## Frontend Interface

The frontend provides a visual interface for interacting with the distributed system.

### Achievements

* Client interaction layer
* Request submission interface
* Distributed response visualization
* System interaction monitoring

---

## Dockerized Infrastructure

The system uses Docker containers for deployment and orchestration.

### Achievements

* Multi-container deployment
* Isolated service execution
* Reproducible distributed environment
* Simplified scalability and testing

---

# Technical Concepts Demonstrated

This project demonstrates practical implementation of several important distributed systems concepts:

* RAFT Consensus Algorithm
* Distributed Coordination
* Leader Election
* Replicated State Machines
* Fault-Tolerant Systems
* Service Communication
* Microservice Architecture
* Distributed Networking
* Container Orchestration
* System Reliability

---

# Educational Value

MiniRAFT provides hands-on exposure to how modern distributed systems such as distributed databases, cloud infrastructure, and consensus-based platforms maintain consistency and reliability.

The project simulates real-world distributed computing principles used in:

* Distributed Databases
* Kubernetes Control Planes
* Blockchain Networks
* Cloud Infrastructure
* High Availability Systems
* Replicated Storage Systems

---

# Technologies Used

* Node.js
* Docker
* Docker Compose
* Distributed System Architecture
* RAFT Consensus Principles
* JavaScript
* Frontend Web Technologies

---

# Why This Project Matters

Distributed consensus is one of the most important problems in modern computing. This project demonstrates an understanding of:

* Building reliable distributed applications
* Coordinating multiple networked nodes
* Maintaining consistency across replicas
* Designing scalable backend architectures
* Handling distributed communication patterns

MiniRAFT serves as a strong academic and portfolio project for showcasing knowledge in distributed systems and backend infrastructure engineering.

---

# Author

Maanya Bhardwaj

GitHub: [https://github.com/maanya-bhardwaj](https://github.com/maanya-bhardwaj)
