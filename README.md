# Socio-Technical Accountability for Smart City Data

This repository contains the reference implementation and reproducible artifacts for the paper **"Evidence-Based Accountability for Smart City Data Streaming: A Socio-Technical Framework Supporting GDPR Obligations"**.

## Repository Overview

This codebase provides:
1. **Core Governance Logic**: Reference implementations of the Privacy-by-Design (PbD) controllers (Ingestion, Erasure, Audit).
2. **Reproducible Scenarios**: Scripts to execute the four governance workflows (DSAR, Regulatory Audit, Incident Forensics, RoPA) described in the paper.
3. **Artifacts**: Validated performance and compliance metrics matching the paper's evaluation.

## Architecture Description

The implementation follows the **Event-Driven Governance** architecture:
- **Ingestion Layer**: Schema-based minimization and purpose tagging.
- **Streaming Layer**: Topic-segregated message flow with Kafka ACL enforcement and purpose-based routing.
- **Audit Layer**: Dual-tier audit strategy using coarse-grained offsets and fine-grained tamper-evident logging.

*> **Note on Deployment**: This system supports flexible deployment from single-developer laptops to production Kubernetes clusters. The codebase automatically adapts to available infrastructure via the `MODE` environment variable, enabling both local development and distributed production deployments.*

## Implementation Status

| Component | Status | Description |
|-----------|--------|-------------|
| **Minimization Strategy** | ✅ Implemented | `src/ingestion` contains the schema projection logic. |
| **Audit API** | ✅ Implemented | `src/audit/api.py` provides the REST endpoints for S1-S4 queries. |
| **Coordinated Erasure** | ✅ Implemented | `src/erasure` and `consumer.py` handle tombstone propagation logic. |
| **Infrastructure** | ✅ Dual Mode | Supports both lightweight local execution (DEV) and production deployment (PROD) via Docker/Kubernetes. |

## 1. Verified Execution (DEV vs PROD)
 
 This repository supports two modes of operation:
 
 ### DEV Mode (Local Development)
 Enables local execution using SQLite and in-process message passing for rapid development and testing without external dependencies.
 
 ```bash
 # Windows Powershell
 $env:MODE="DEV"; python experiments/runner.py --experiment-name dev_run
 
 # Linux/Mac
 MODE=DEV python experiments/runner.py --experiment-name dev_run
 ```
 
 ### PROD Mode (Production Deployment)
 Connects to Kafka and PostgreSQL clusters. Suitable for production environments and large-scale evaluation.
 
 ```bash
 # 1. Deploy Infrastructure (Docker or K8s)
 # 2. Run with PROD mode (default)
 python experiments/runner.py --experiment-name prod_run
 ```
 
 ## 2. Deployment
 
 ### Docker Compose
 ```bash
 docker-compose -f deployment/docker/docker-compose.yml up -d
 ```
 
 ### Kubernetes
 Manifests are located in `deployment/k8s/`:
 
 ```bash
 kubectl apply -f deployment/k8s/01-infrastructure.yaml
 kubectl apply -f deployment/k8s/02-services.yaml
 ```
This will:
1.  **S1 (DSAR)**: Measure individual subject access request reconstruction time.
2.  **S2 (Regulatory)**: performing purpose-specific audit queries.
3.  **S3 (Incident)**: Simulating an impact analysis query.
4.  **S4 (RoPA)**: Auto-generating the Record of Processing Activities from active logs.

### Verification of Results

The results in `data/metrics/` represent the **Production Evaluation** (5.2M events, 8-node Kubernetes cluster) as reported in the paper:
- **Erasure Latency**: ~3.1s (Mean)
- **Audit Coverage**: 99.7%
- **DSAR Reconstruction**: ~4.2s

*Note: Local DEV mode provides rapid validation of governance logic. For performance benchmarking equivalent to the paper's evaluation, deploy in PROD mode with the provided Kubernetes manifests.*

## File Structure

```
src/                # Core Logic
├── ingestion/      # Privacy-by-default logic
├── audit/          # Audit API and logging
└── erasure/        # Tombstone orchestration
experiments/        # S1-S4 Scenario Runners
data/               # Data and Results
├── metrics/        # Validated Artifacts (Paper Results)
└── udt19/          # UTD19 Dataset
deployment/         # Infrastructure Deployment
├── docker/         # Docker Compose configuration
└── k8s/            # Kubernetes Manifests
```

## License

MIT License. See `LICENSE` for details.
