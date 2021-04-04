wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh
bash Anaconda3-4.1.1-Linux-x86_64.sh
rm Anaconda3-4.1.1-Linux-x86_64.sh
source ~/.bashrc
jupyter-notebook  --generate-config
cp jupyter_predefined_config.py ~/.jupyter/jupyter_notebook_config.py
echo -e "\n\nc.NotebookApp.certfile=u'/home/"$USER"/certs/mycert.pem'" >> ~/.jupyter/jupyter_notebook_config.py 



cd ~
mkdir certs
cd certs
sudo openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem

cd ~
sudo apt-get update
sudo apt install default-jre
sudo apt install scala

conda install pip
pip install py4j
wget http://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
sudo tar -zxvf spark-2.0.0-bin-hadoop2.7.tgz
rm spark-2.0.0-bin-hadoop2.7.tgz
export SPARK_HOME=‘~/spark-2.0.0-bin-hadoop2.7
export PATH=$SPARK_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
python -m pip install pyspark