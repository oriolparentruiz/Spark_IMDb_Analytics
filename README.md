# ⚡ Sequential Python vs Apache Spark: Performance Comparison on IMDb Data

Performance and scalability comparison of the same analytical application implemented three ways (**sequential Python**, **Spark RDD** and **Spark DataFrame**) and run on a 4-node Apache Spark cluster on AWS EC2, with the aim of finding the data size at which distributed processing starts to pay off.

---

## 📌 Overview

**Application:** from a dataset derived from IMDb, find the **actor–director pairs that have worked together on more than one film**. For each pair the program computes the number of collaborations and the average, minimum and maximum rating of the films they share, then outputs the **top 20 pairs** ordered by number of shared films.

**Dataset:** `infoActors.csv`, about 2.3 million rows. Each row is one actor's appearance in a film, with `tconst`, `actor`, `actorName`, `director`, `directorName`, `averageRating` and `numVotes`. To study scalability, enlarged versions were built by controlled concatenation of the original file:

| Version | Size |
|---|---|
| **Original** | Base file from IMDb |
| **x3** | Original concatenated 3 times |
| **x6** | Original concatenated 6 times |
| **x9** | Original concatenated 9 times |

**Cluster:** 4 Amazon EC2 `m5.large` instances (2 vCPUs, 8 GB RAM, 30 GB storage each) running Ubuntu, with a shared security group for Spark networking. One node acts as **master** and the other three as **workers**. Software and passwordless SSH between nodes are installed with the `automatic_deploy.sh` script.

**Measurement:** total execution time (*elapsed time*) measured with `/usr/bin/time`, together with user time, system time and CPU usage. All versions were run locally for initial tests and on the cluster with `spark-submit` pointing to the master URL.

## 🔬 Implementations and findings

| Version | What was done | Main findings |
|---|---|---|
| **Python (sequential)** | Dictionary-based aggregation reading the CSV with Python's `csv` module, on a single core. | Fastest on the original dataset (12.6 s). Time grows almost linearly with size, with ~99% CPU and negligible system overhead, so it becomes inefficient as data grows. |
| **Spark RDD** | `map` to key-value pairs, `reduceByKey` to aggregate count, sum, min and max, then filter, average and sort. | Significant start-up overhead even for small data. Scales better than Python only on paper: CPU usage drops from 58% to 19% from original to x9, so most of the time goes to communication and synchronisation. It is the slowest version at x9 (125.7 s). |
| **Spark DataFrame** | `groupBy` + `agg` (count, avg, min, max), filter on `count >= 2`, `orderBy` and `limit(20)`. | Best scalability. User CPU time stays around 24–25 s regardless of data size, and CPU usage drops much less than with RDD. Fastest option from x6 onwards. |

### Execution results

| Size | Version | User (s) | System (s) | Elapsed (s) | %CPU |
|---|---|---|---|---|---|
| Original | Python | 12.15 | 0.44 | **12.60** | 99% |
| Original | RDD | 22.54 | 0.87 | 39.74 | 58% |
| Original | DataFrame | 24.71 | 0.87 | 31.16 | 82% |
| x3 | Python | 37.80 | 0.55 | 38.36 | 99% |
| x3 | RDD | 23.70 | 0.91 | 66.50 | 37% |
| x3 | DataFrame | 24.95 | 0.82 | **36.15** | 71% |
| x6 | Python | 73.21 | 0.72 | 73.94 | 99% |
| x6 | RDD | 23.99 | 0.91 | 94.51 | 26% |
| x6 | DataFrame | 24.62 | 0.86 | **42.69** | 59% |
| x9 | Python | 109.78 | 0.82 | 110.61 | 99% |
| x9 | RDD | 23.50 | 0.84 | 125.70 | 19% |
| x9 | DataFrame | 25.85 | 0.90 | **59.14** | 45% |

**Inflection point:** around **x3**, where Spark DataFrame roughly matches sequential Python (36.2 s vs 38.4 s). From x6 on, DataFrame is clearly faster, and at x9 it is about **47% faster than sequential Python** and **53% faster than RDD**.

**Overall conclusion:** there is no single best solution. Distributed processing is not always the right choice, because for small inputs the cost of starting and coordinating the cluster outweighs the benefits. Once the data is large enough, **Spark DataFrame** is the most efficient and scalable option, helped by the optimisations of the Catalyst engine that the RDD API lacks.

## 📁 Repository structure

```
.
├── python_actorDirector.py   # Sequential Python version (csv module)
├── RDD_actorDirector.py      # Spark version using the RDD API
├── DF_actorDirector.py       # Spark version using the DataFrame API
├── automatic_deploy.sh       # Cluster setup script (Spark + SSH keys)
├── infoActors.csv            # Input dataset (read by the three programs)
└── README.md
```

> ⚠️ The three programs read `infoActors.csv` from the working directory. The dataset and `automatic_deploy.sh` were not included with the uploaded files, so add them to the folder (the dataset is derived from the public IMDb data) before running.

## ▶️ How to reproduce

**Requirements:** Python 3, Apache Spark with PySpark, and (for the distributed runs) a Spark cluster, e.g. 4 EC2 instances set up with `automatic_deploy.sh`.

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/<repo-name>.git
   cd <repo-name>
   ```
2. Add `infoActors.csv` to the folder. To build the larger versions, concatenate the original file (keeping a single header line) to get the x3, x6 and x9 variants.
3. Run the sequential version:
   ```bash
   /usr/bin/time -v python3 python_actorDirector.py
   ```
4. Run the Spark versions, locally or against the cluster master:
   ```bash
   # Local mode
   /usr/bin/time -v spark-submit RDD_actorDirector.py
   /usr/bin/time -v spark-submit DF_actorDirector.py

   # Cluster mode
   /usr/bin/time -v spark-submit --master spark://<master-ip>:7077 DF_actorDirector.py
   ```
5. Compare the *elapsed time*, user/system time and CPU percentage reported by `/usr/bin/time` across versions and file sizes.

## 🛠️ Tools

- **Python** · `csv` module for the sequential baseline
- **Apache Spark / PySpark** · RDD API (core Spark) and DataFrame API (Spark SQL)
- **AWS EC2** · 4 × `m5.large` instances running Ubuntu (1 master + 3 workers)
- **`/usr/bin/time`** · execution time, user/system time and CPU usage

## 📚 References

- Apache Software Foundation (2024). *Apache Spark™ - Unified Analytics Engine for Big Data*. https://spark.apache.org/
- Course notes and lab guides, *Software for Data Analysis: Spark*, UPC, 2024–2025.

## 👥 Authors

- **Oriol Parent**
- **Lucas Viner**
