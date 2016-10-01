# S2Graph Development

## RAT

[Apache RAT](https://creadur.apache.org/rat/apache-rat/index.html) is an audits software tool.

To run it agains S2Graph source code, follow these steps:
    
    cd dev
    wget https://repo1.maven.org/maven2/org/apache/rat/apache-rat/0.12/apache-rat-0.12.jar
    ./run-rat.sh apache-rat-0.12.jar

The toll will write a report in case it finds any issue.
