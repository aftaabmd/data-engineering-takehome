#!/usr/bin/env python
# coding: utf-8

# In[16]:


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 11:42:14 2023

@author: aftaabmohammed
"""


import pandas
from matplotlib import pyplot as plt


data_cus=pandas.read_csv("sample_data/Customer.txt", sep="|", header=None, names=["Column1", "Column2","Column3", "Column4"])
data_cus_ex=pandas.read_csv("sample_data/Customer_Extended.txt", sep="|", header=None)
data_refund=pandas.read_csv("sample_data/Refund.txt", sep="|", header=None)
data_sales=pandas.read_csv("sample_data/Sales.txt", sep="|", header=None, names=["Column1", "Column2","Column3", "Column4","Column5","Column6"])
data_product=pandas.read_csv("sample_data/Product.txt", sep="|", header=None, names=["Column1", "Column2","Column3", "Column4","Column5"])



# In[17]:


data_cus.head()


# In[21]:


data_product.head()


# In[22]:


data_sales.head()


# In[35]:


data_sales_groupby = data_sales.groupby("Column3")["Column5"].count().to_frame(name = 'count').reset_index()

data_prod_2 = data_product.join(data_sales_groupby["count"])


# In[46]:


from matplotlib import pyplot as plt



# In[53]:


data_prod_2.plot(x="Column2", y=["count"], kind="bar", stacked = True)


# In[40]:


data_prod_2


# In[60]:


data_sales["Column4"] = pandas.to_datetime(data_sales["Column4"])


# In[59]:


mask = (df['Column'] > ) & (df['date'] <= end_date)


# In[71]:


data_sales["Column3"].unique()


# In[67]:


data_prod_2["Column1"].unique()


# In[72]:


data_cus_ex.head()


# In[ ]:




