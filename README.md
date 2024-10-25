# aos-lms-milestone-3

This is the entire code for the implementation of the AOS milestone 3. RAFT Cluster integration into LMS system previously built.

## Setup

### Prepare the Environment
Setup environment
```bash
python -m venv .venv;
.venv\Script\activate;
python -m pip install -r requirements.txt
```

### To start RAFT Cluster
Currently the cluster size is 3, it can be run using the following commands :

```bash
python .\lms_raft_server.py --sid=0 --self_address=localhost:50000 --peers="localhost:50001,localhost:50002" --llm="localhost:50051"
python .\lms_raft_server.py --sid=1 --self_address=localhost:50001 --peers="localhost:50000,localhost:50002" --llm="localhost:50051"
python .\lms_raft_server.py --sid=2 --self_address=localhost:50002 --peers="localhost:50001,localhost:50000" --llm="localhost:50051"
```

---

### To start the LLM Server
```bash
python ./llm_server.py
```
---
To start the client
```bash
python ./lms_raft_client.py
```
---
To start the CLI client
```bash
streamlit run ./lms_raft_gui_client.py
```