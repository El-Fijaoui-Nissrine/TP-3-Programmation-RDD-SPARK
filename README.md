# TP-3-Programmation-RDD-SPARK

A collection of Apache Spark applications demonstrating RDD operations for data analysis using Java.

## Overview

This repository contains two exercises focusing on Resilient Distributed Datasets (RDD) programming with Apache Spark:

1. **Sales Data Analysis** - Aggregate sales by city and year
2. **Web Server Log Analysis** - Parse and analyze Apache access logs

## Prerequisites

- Java 11
- Apache Spark 3.5.0
- Maven (for dependency management)

## Exercise 1: Sales Analysis

### Description
Analyzes sales data from a text file to compute total sales by city and by city-year combination.

### Input Format
```
date city product price
```

Example:
```
2025-01-01 Paris ProduitA 100
2024-01-02 Lyon ProduitB 150
2025-01-03 Paris ProduitC 200
```

### Features
- Calculate total sales per city
- Calculate total sales per city and year
- Local execution mode for testing


## Exercise 2: Apache Log Analysis

### Description
Processes web server access logs to extract insights about traffic patterns, errors, and popular resources.

### Input Format
Example:
```
127.0.0.1 - - [10/Oct/2025:09:15:32 +0000] "GET /index.html HTTP/1.1" 200 1024 "http://example.com" "Mozilla/5.0"
192.168.1.10 - john [10/Oct/2025:09:17:12 +0000] "POST /login HTTP/1.1" 302 512 "-" "curl/7.68.0"
```

### Features
- **Basic Statistics**: Total requests, error count, error percentage
- **Top 5 IPs**: Most active client addresses
- **Top 5 Resources**: Most requested URLs
- **HTTP Code Distribution**: Breakdown of response codes (200, 404, 500, etc.)



## Project Structure
<img width="475" height="381" alt="image" src="https://github.com/user-attachments/assets/b35a8f19-8b96-40bf-8f1a-bb8ff5684862" />




##  Output

### Sales Analysis
<img width="472" height="265" alt="image" src="https://github.com/user-attachments/assets/2d14d25f-b64f-4ccf-9a9b-5f8ea897495d" />


### Log Analysis
<img width="333" height="524" alt="image" src="https://github.com/user-attachments/assets/5d59d2e7-94d0-45e1-94f3-6b3c56a8ff8c" />


