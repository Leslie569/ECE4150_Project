cd ~
sudo apt-get update
sudo apt install default-jre scala python3-pip -y

wget https://apache.claz.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
sudo tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz
mkdir ~/opt
mv spark-3.1.1-bin-hadoop2.7 ~/opt/spark

echo "export SPARK_HOME=~/opt/spark" >> ~/.bashrc
echo "export PATH=$PATH:~/opt/spark/bin:~/opt/spark/sbin" >> ~/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bashrc
rm spark-3.1.1-bin-hadoop2.7.tgz

sudo apt install python3-pip
python3 -m pip install findspark