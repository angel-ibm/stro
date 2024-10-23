# Anomaly dection using Milvus, Presto and Kafka on a Lakehouse

![Top](./images/cover1.png "watsonxdata")

We all know that beautiful pictures often captivate us, though this beauty may sometimes be deceiving. For not making it personal, take, for example, the night sky: a source of inspiration and wonder beneath the moonlight, yet it may conceal unseen dangers.

## A big family of use cases

There are numerous examples where anomaly detection is critical, not only for a company's success but also for the well-being of society. Some key areas include:

- Quality control in the automotive industry
- Detecting fabric defects in retail
- Surface deterioration in civil engineering
- Defect detection in electronics
- Assisted disease diagnosis and alike in life sciences
- ...and many more, such as in agriculture, surveillance, and beyond.


For this demo, the use case we will present is of monumental importance. Indeed, the threat it addesses could compromise the very existence of humanity: the detection of asteroids or near-Earth objects with potentially devastating consequances if a collision might occur.

Don't think about science fiction now, this is no kidding at all. In fact,  significant resources have been, and continue to be, invested in identifying potentially hazardous objects in space. Projects like the [NEO Surveyor ](https://science.nasa.gov/mission/neo-surveyor/), the [Space Mision DART](https://science.nasa.gov/mission/dart/) , and the [Flyeye project](https://www.esa.int/ESA_Multimedia/Images/2016/10/Flyeye_telescope) are dedicated to detecting these threats and protecting the Eartht from potential risks.

<!-- Row 0 -->
<div style="font-family: 'IBM Plex Sans';">
<table style="float:left; width: 620px; height: 235px; border-spacing: 10px; border-collapse: separate; table-layout: fixed">
    <td style="padding: 15px; text-align:left; vertical-align: text-top; background-color:#F7F7F7; width: 300px; height: 250px;">
        <div style="height: 75px"><p style="font-size: 24px">
<!-- Title -->
NEO Surveyor Project
        </div>
        <div style="height: 125px"><p style="font-size: 14px">
<!-- Description -->
Near-Earth Object (NEO) Surveyor is the first space telescope specifically designed to hunt asteroids and comets that may be potential hazards to Earth.
        </div>
        <div style="height: 25px"><p style="font-size: 12px; text-align: right">
<!-- Duration -->            
        </div>
        <div style="height: 10px"><p style="font-size: 12px; text-align: right">
<!-- URL -->
<a href="https://science.nasa.gov/mission/neo-surveyor/">
        <img style="display: inline-block;"src="./images/arrowblue.png"></a>          
        </div>        
    </td>    
    <td style="padding: 15px; text-align:left; vertical-align: text-top; background-color:#F7F7F7; width: 300px; height:250px">
        <div style="height: 75px"><p style="font-size: 24px">
<!-- Title -->            
Space Mission DART
</div>
        <div style="height: 125px"><p style="font-size: 14px">
<!-- Abstract -->
DART (Double Asteroid Redirection Test ) was the first-ever mission dedicated to investigating and demonstrating one method of asteroid deflection by changing an asteroid’s motion in space through kinetic impact.
        </div>
        <div style="height: 25px"><p style="font-size: 12px; text-align: right">
        </div>  
        <div style="height: 10px"><p style="font-size: 12px; text-align: right">
<!-- URL -->            
<a href="https://science.nasa.gov/mission/dart/">
                 <img style="display: inline-block;"src="./images/arrowblue.png"></a>            
        </div>            
    </td>
    <td style="padding: 15px; text-align:left; vertical-align: text-top; background-color:#F7F7F7; width: 300px; height: 250px;">
        <div style="height: 75px"><p style="font-size: 24px">
<!-- Title -->
Flyeye Observatory
        </div>
        <div style="height: 125px"><p style="font-size: 14px">
<!-- Description -->
As part of the global effort to spot risky celestial objects such as asteroids and comets, ESA is developing a network of automated telescopes for nightly sky surveys. The ‘Flyeye-1’ telescope is the first in a future network that would scan the entire sky and automatically identify possible new near-Earth objects (NEOs) for follow up and later checking by human astronomers.
        </div>
        <div style="height: 25px"><p style="font-size: 12px; text-align: right">
<!-- Duration -->            
        </div>
        <div style="height: 10px"><p style="font-size: 12px; text-align: right">
<!-- URL -->
<a href="https://www.esa.int/ESA_Multimedia/Images/2017/02/Flyeye_Observatory">
        <img style="display: inline-block;"src="./images/arrowblue.png"></a>          
        </div>        
    </td>
      
</table>
</div>  

![Missions](./images/missions.png)

## Objectives

The motivation of the demo is to illustrate how a group of technologies can be easily integrated  to extract valuable insights from the combination of graphical content and its metadata (with indepence of the use case or the industry area). It is not intended to represent an actual implementation of near-Earth object detection. No, astronomers do not operate exactly in this way. However, this is not an obstacle to learn how to how to effectively combine multiple technological components to achieve useful outcomes. 

Consider a project that requires:

- Unstructured mass storage in a Lakehouse
- Extraction and management of content metadata
- Event generation and data transport
- Embedding generation and similarity search

Milvus, Kafka, Presto, Iceberg, Python are good candidates to address the requirements mentioned above. They are integrated and demonstrated in the following sections.  The demo is hosted in an environment created by IBMers called [watsonx.data Lab](https://ibm.github.io/watsonx-data-lab/) aimed at educating other IBMers, Business Partners, and anyone with access to the [IBM TechZone](https://ibm.github.io/watsonx-data-lab/). This environment extensively leverages the commercial product [watsonx.data](https://www.ibm.com/products/watsonx-data) which combines many technologies typically present in projects dedicated to analytics and artifical intelligence. Take a look at the [watsonx.data Solution Brief](https://www.ibm.com/downloads/cas/4Z1YXEBO) for more details on this product.

## The story

<img src="./images/ergenzingen.jpg" alt="Ergenzingen" title="ergenzingen" style="width:50%; float:left; margin-right:10px;">

 Professional and amateur astronomers often compare their observations (i.e: their own sky images) with established scientific databases that catalog all known celestial objects. Examples of these databases include [GAIA](https://www.cosmos.esa.int/web/gaia/), the [Horizons System](https://ssd.jpl.nasa.gov/horizons/) and the [Minor Planet Center](http://www.minorplanetcenter.net/about). These systems house billions of entries and petabytes of data. Indeed, the universe is vast.

When astronomers encounter an unidentified object, it's considered an anomaly. They continue to observe and calculate its trajectory to determine whether it poses any concern. These anomalies can range from insignificant issues like space debris, camera malfunctions or atmospheric interference to potentially significant discoveries such as previously unnoticed asteroids or comets. One example of a surveillance system used for this purpose is [Scout: NEOCP Hazard Assessment](https://cneos.jpl.nasa.gov/scout/intro.html).
