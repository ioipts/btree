# Self-Adaptive B-Tree
  
ต้นไม้แบบบีที่สามารถ รักษาสมดุลในตัวเองได้ เมื่อมีการใส่ข้อมูล หรือลบข้อมูล   
ต้นไม้จะปรับตัวเองเพื่อให้ค้นหาได้รวดเร็ว ไม่เอียงไปข้างใดข้างนึง  
ไม่ว่าข้อมูลที่ได้รับจะมีการเรียงมากก่อน (เช่น เวลา)  
โค้ดส่วนนี้จะใช้สำหรับสร้าง ไฟล์สำหรับค้นหา (เช่นเดียวกับที่ใช้ในฐานข้อมูล)  
เหมาะสำหรับงานที่ต้องการค้นหาในไฟล์ด้วยความเร็ว แต่มีข้อจำกัดเรื่องขนาดหน่วยความจำ หรือขนาดโปรแกรม  เช่นอุปกรณ์ IoT ที่ไม่สามารถลงฐานข้อมูลที่มีขนาดใหญ่และกินเนื้อที่ได้

This little module will create an index file for searching.  
The algorithm is called Self-balancing B-Tree.  
It can adapt by itself to serve random data and sorted data (both ascending and descending).  
The length of the key and number of the key per node can be configured.  
This module is suitable for IoT project where database is not fit.

## Feature
Written in C/C++98.  
Support multi-platform.  
Support both 32-bit and 64-bit.
Operation :Insert, Delete, Update, Upsert, Query distinct, Query between, Count between  
  
## Performance
Tested on Apple MacBook M1 with sorted key (Complied for ARM) 
  
Bulk insert with sorted key  
10M records in 91sec with output size 670Mbytes  
100M records in 1000sec with output size 6.8Gbytes  
  
Multi-thread query   
600K queries in 1 sec  

## How to use
Just copy all files and include in the project.  
Enjoy!  

## How to run the sample
cd sample  
make  
./testbtree
