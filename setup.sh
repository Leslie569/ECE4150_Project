wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
bash Anaconda3-2020.11-Linux-x86_64.sh
cd ~
./anaconda3/bin/jupyter-notebook  --generate-config
cp ECE4150_Project/jupyter_predefined_config.py ~/.jupyter/jupyter_notebook_config.py
echo -e "\n\nc.NotebookApp.certfile=u'/home/"$USER"/certs/mycert.pem'" >> ~/.jupyter/jupyter_notebook_config.py 

cd ~
mkdir certs
cd certs
sudo openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem
sudo chown ubuntu:ubuntu mycert.pem

cd ~
sudo apt-get update
sudo apt install default-jre
sudo apt install scala

conda install pip
pip install py4j
wget http://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
sudo tar -zxvf spark-2.0.0-bin-hadoop2.7.tgz
rm spark-2.0.0-bin-hadoop2.7.tgz

echo -e "\nexport SPARK_HOME='~/spark-2.0.0-bin-hadoop2.7'\nexport PATH=$SPARK_HOME:/home/ubuntu/anaconda3/bin:$PATH\nexport PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH" >> ~/.bashrc
./anaconda3/bin/python -m pip install --no-cache-dir pyspark