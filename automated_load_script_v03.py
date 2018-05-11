
# coding: utf-8

# In[1]:


from datetime import datetime
import pandas as pd


# # User Input

# In[2]:


period_name = input("Enter the period name, ie: 1703, 1608 ... : \n")
start_day = input("Enter the first day of your period, with the format YYYY/MM/DD : \n")
end_day = input("Enter the last day of your period, with the format YYYY/MM/DD: \n")
aws_access_key_id = input("Enter your aws access key ID : \n")
aws_secret_access_key = input("Enter your aws secret access key: \n")


# In[3]:


period = pd.date_range(start_day, end_day)


# # Create Cell reference and range loading script

# In[4]:


cell_ref = []
cell_range = []

for p in period:
    m = str(p.month).zfill(2)
    d = str(p.day).zfill(2)
    
    d_name = str(p.year) + m + d
    d_slash = '/'.join([str(p.year), m, d])
    
    cell_ref.append('CREATE TABLE IF NOT EXISTS playpen_vtec.cell_reference_%s (country VARCHAR(2), source_cell_id VARCHAR(14), lac VARCHAR(10),cell_id VARCHAR(10), lat_wgs84 FLOAT, lon_wgs84 FLOAT, technology VARCHAR(2), site_type VARCHAR(5), sporadic BOOLEAN, azimuth FLOAT, beamwidth FLOAT, frequency INT, antenna_height INT, cell_power INT,  cell_range INT, service_to VARCHAR(10), cell_index INT); ' % d_name)
    
    cell_ref.append("COPY playpen_vtec.cell_reference_%s FROM 's3://tef.prod.uk/core/5/cip/common/extended_ucr/0/%s/csv_3_0/' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '|' bzip2; " %(d_name, d_slash, aws_access_key_id, aws_secret_access_key))
    
    cell_ref.append('grant all privileges on playpen_vtec.cell_reference_%s to public; \n'%d_name )
    
    
    cell_range.append('CREATE TABLE IF NOT EXISTS playpen_vtec.cell_range_%s (cell_id INT  NOT NULL,  technology TEXT  NULL, site_type  TEXT  NULL, utm_x INT  NOT NULL, utm_y INT  NOT NULL, utm_zone TEXT NOT NULL, azimuth REAL  NULL, beamwidth  REAL  NULL, cell_range REAL  NULL, service_to TEXT  NULL, cell_index SMALLINT NULL, source_cell_id  TEXT  NULL );' % d_name)
    
    cell_range.append("COPY playpen_vtec.cell_range_%s from 's3://tef.prod.uk/core/5/cip/common/daily_cell_record/0/%s/csv_2_1/' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '|' bzip2; " %(d_name, d_slash, aws_access_key_id,aws_secret_access_key))
    
    cell_range.append('grant all privileges on playpen_vtec.cell_range_%s to public; \n'%d_name )


# In[5]:


cell_ref_print = '\n'.join(cell_ref).replace('\t', ' ')
cell_range_print = '\n'.join(cell_range).replace('\t', ' ')


# In[6]:


with open(r'./loading_scripts/%s.sql' % ('p'+str(period_name)), 'w') as sql_script:
    sql_script.write(cell_range_print)
    sql_script.write(cell_ref_print)


# # Create uners loading script

# In[7]:


source_bool = input("Different sources for uners ? [y/n] : \n")


# In[8]:


if source_bool == 'y':
    s3_path_check = raw_input("Are the two paths: \n s3://tef.prod.uk/core/5/cip/common/uner/0/ \n  s3://tef.prod.uk/core/ukvtec5/cip/common/uner/0/ \n ? \n[y/n]")
    if  s3_path_check == 'y':
        ukvtec_start_day = raw_input('Start day for loading from s3://tef.prod.uk/core/ukvtec5/cip/common/uner/0/ [YYYY/MM/DD] : \n')
        ukvtec_end_day = raw_input('End day for loading from s3://tef.prod.uk/core/ukvtec5/cip/common/uner/0/ [YYYY/MM/DD] : \n')
        cip_start_day = raw_input('Start day for loading from s3://tef.prod.uk/core/cip/common/uner/0/ [YYYY/MM/DD] : \n')
        cip_end_day = raw_input('End day for loading from s3://tef.prod.uk/core/cip/common/uner/0/ [YYYY/MM/DD] : \n')
        period_ukvtec = pd.date_range(ukvtec_start_day, ukvtec_end_day)
        period_cip = pd.date_range(cip_start_day,cip_end_day)
else:
  period_cip = period
  period_ukvtec = []


# In[9]:


uners = []

for p in period:
  
    m = str(p.month).zfill(2)
    d = str(p.day).zfill(2)
    
    d_name = str(p.year) + m + d
    
    uners.append('CREATE TABLE IF NOT EXISTS playpen_vtec.uner_%s (source TEXT, user_id  BIGINT, event_type TEXT, native_event_type TEXT, start_timestamp TIMESTAMP, cell_id TEXT, device_id TEXT, device_type_id TEXT, country_code TEXT); '%d_name) 




# In[12]:


for p in period_cip:
  
    m = str(p.month).zfill(2)
    d = str(p.day).zfill(2)
    
    d_name = str(p.year) + m + d
    d_slash = '/'.join([str(p.year), m, d])
    
    uners.append("COPY playpen_vtec.uner_%s from 's3://tef.prod.uk/core/5/cip/common/uner/0/%s/csv_4_0/' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '|' bzip2 ;" %(d_name, d_slash, aws_access_key_id,aws_secret_access_key)) 

if len(period_ukvtec) != 0:
    for p in period_ukvtec:
  
        m = str(p.month).zfill(2)
        d = str(p.day).zfill(2)
        
        d_name = str(p.year) + m + d
        d_slash = '/'.join([str(p.year), m, d])
        
        uners.append("\nCOPY playpen_vtec.uner_%s  from 's3://tef.prod.uk/core/ukvtec5/cip/common/uner/0/%s/csv_4_0/' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '|' bzip2 ; " %(d_name, d_slash, aws_access_key_id, aws_secret_access_key)) 
  


# In[13]:


for p in period:
  
    m = str(p.month).zfill(2)
    d = str(p.day).zfill(2)
    
    d_name = str(p.year) + m + d
    
    uners.append('GRANT ALL PRIVILEGES ON playpen_vtec.uner_%s TO public;' %d_name) 


# In[14]:


uners_print = '\n'.join(uners)
with open(r'./loading_scripts/%s.sql' % ('p'+str(period_name)), 'a') as sql_script:
    sql_script.write(uners_print)

