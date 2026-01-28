#!/bin/bash
source /home/azureuser/project1/.venv/bin/activate
cd /home/azureuser/ICT-Project2-KOR
streamlit run app.py --server.port 8505 --server.address 0.0.0.0
