# FastAPI

To install Uvicorn and use FastAPI in a virtual machine with limited resources, the following commands were used:

* pip3 install fastAPI(all)
* uvicorn main:app --host 0.0.0.0 --port 8000

This commands install fastAPI and Uvicorn. The necesary libraries for the API were already installed

After this, fastAPI can be used by entering to "[Virtual-machine-ip]:8000//seller/[seller_id]" and this will return all the KPIs for that seller.