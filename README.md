# deep_engine
High-performance C++ database storage engine

# Required packages
sudo apt-get install g++ <br />
sudo apt-get install cmake <br />
sudo apt-get install maven <br />
sudo apt-get install zlib1g-dev <br />
sudo apt-get install openjdk-8-jdk

# 1st initialize maven
cd ${ROOT}/build/maven <br />
mvn clean install

# 2nd build source
cd ${ROOT} <br />
mvn clean install -Dmaven.test.skip=true

# 3rd execute tests
cd ${ROOT}/ENGINE <br />
mvn test
