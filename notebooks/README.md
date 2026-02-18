# LEASE Data Analytics Notebooks

## Introduction

This directory contains a collection of Jupyter notebooks designed to provide direct, query-level access to the cleaned leasehold dataset. These notebooks serve as an analytics environment for technical users at LEASE and ODI's team to run experiments on user queries and explore spatial data integration.

## Purpose

The notebooks in this collection demonstrate how to perform common analyses on the leasehold data, including:

- **Identifying hotspots of short leases** – Discover geographic concentrations of properties with lease terms below critical thresholds
- **Exploring correlations with other datasets** – Integrate and analyse leasehold data alongside external spatial and demographic data
- **Running custom queries** – Execute ad-hoc analyses tailored to specific research questions

## Strategic Value

This analytics environment is more than a business intelligence tool—it is a **strategic asset for LEASE**. 

The Phase 1 report recommended that LEASE use analytical findings to "advocate for targeted digitisation" with HMLR. These notebooks provide the means to do exactly that. With this environment, LEASE can:

- **Independently explore the data** without relying on external technical support
- **Generate statistics** to understand leasehold patterns across England and Wales
- **Build evidence-based cases** for policy and advocacy work
- **Operationalise recommendations** from the initial project phase

## Getting Started

Each notebook is self-contained with documentation explaining its purpose and how to use it. Start with the introductory notebooks to familiarise yourself with the data structure before moving on to more advanced analyses.

## Requirements

- Python 3.10+
- Jupyter Notebook or JupyterLab
- MongoDB connection to the leasehold database
- Required packages as specified in `pyproject.toml`

## Before You Begin

1- Create a Python virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

python3 -m ipykernel install --user --name=venv
```

2- Start Jupyter Notebook:

```bash
jupyter notebook
```
