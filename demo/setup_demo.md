# Welcome

This Demo Script can be found here:
<https://github.com/angel-ibm/stro>

and it is based on the technologies described on this lab:
<https://ibm.github.io/watsonx-data-lab/>

## Prerequisites

You need to have access to the IBM TechZone <https://techzone.ibm.com> and need to provision this demo environment: <https://ibm.github.io/watsonx-data-lab/wxd-reference-techzone/>.

The important sections are:

- Jupyter:
<https://ibm.github.io/watsonx-data-lab/wxd-jupyter/>

- Kafka:
<https://ibm.github.io/watsonx-data-lab/wxd-kafka/>

- Milvus:
<https://ibm.github.io/watsonx-data-lab/wxd-milvus/>

Needless to say, the section dedicated to the watsonx.data UI is also essential to exercise the demo. It is recommended to review it and verify that it works as expected <https://ibm.github.io/watsonx-data-lab/wxd-intro-watsonui/>

## Environment Setup

### 1. Jupyter

- Get Jupyter up and running following the instructions of the lab. See:  
  <https://ibm.github.io/watsonx-data-lab/wxd-jupyter/>
  
- Verify that Jupyter works well with watsonx.data running the notebook `Python Example.ipynb`

- Change to the `Jupyter Lab` GUI, not the Jupyter Notebook:
  
![jupy1](../images/jupy1.png "jupy1")

- Open a terminal in Jupyter Lab

![jupy2](../images/jupy2.png "jupy2")

![jupy3](../images/jupy3.png "jupy3")

- Download the demo code. In other words, clone the demo repository by typing on the prompt: `git clone https://github.com/angel-ibm/stro.git`

![jupy4](../images/jupy4.png "jupy4")

- Go into the demo directory (`stro`) that has been automatically created with the clone command

![jupy5](../images/jupy5.png "jupy5")

- Finally, go into the demo directory

![jupy6](../images/jupy6.png "jupy6")


### 2. Milvus

- Ensure to complete the setup of Milvus in the lab image. See:
<https://ibm.github.io/watsonx-data-lab/wxd-milvus/>


- Verify that Milvus works well by running the notebook `Milvus Example.ipynb`

### 3. Kafka

- Prepare Kafka follwing the instructions of the lab. See:
   <https://ibm.github.io/watsonx-data-lab/wxd-kafka/>
  
- Verify Kafka  by running the notebook `Kafka.ipynb`

## Demo Setup

- Open the Jupyter Notebook interface
- Switch to Jupyter Lab
- Open a terminal
- ensure that you are in /notebooks
- Clone the repository: `git clone https://github.com/angel-ibm/stro.git`
- Start the notebook `full_demo.ipynb`
