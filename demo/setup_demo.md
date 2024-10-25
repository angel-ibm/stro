![Top](../images/watsonxdata.png "watsonxdata")

# Demo Setup

We will provision and prepare a watsonx.data system to tun the demo. The full environment is based on the technologies described on this lab:
<https://ibm.github.io/watsonx-data-lab/>

## Prerequisites

You need to have access to the IBM TechZone <https://techzone.ibm.com> and need to provision this demo environment: <https://ibm.github.io/watsonx-data-lab/wxd-reference-techzone/>.

While the system is provisioning, just review the important sections to get a first feeling of what is going to happen next:

- Jupyter:
<https://ibm.github.io/watsonx-data-lab/wxd-jupyter/>

- Kafka:
<https://ibm.github.io/watsonx-data-lab/wxd-kafka/>

- Milvus:
<https://ibm.github.io/watsonx-data-lab/wxd-milvus/>

Needless to say, the section dedicated to the watsonx.data UI is  essential to exercise the demo. After the system has been provisioned, it is recommended to review and verify that the GUI works as expected before anything else: <https://ibm.github.io/watsonx-data-lab/wxd-intro-watsonui/>

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

- Verify that Milvus works well by running the notebook `Milvus Example.ipynb`. It is not necessary to try the remote connection section of the notebook because we will execute everything locally on the environment we have just provisioned.

### 3. Kafka

- Prepare Kafka follwing the instructions of the lab. See:
   <https://ibm.github.io/watsonx-data-lab/wxd-kafka/>
  
- Verify Kafka  by running the notebook `Kafka.ipynb`

