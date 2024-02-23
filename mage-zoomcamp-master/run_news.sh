#!/usr/bin/bash

n=250
while [ $n -le 399 ]
    do
        echo "/usr/local/bin/mage run magic-zoomcamp news_to_gcs --runtime-vars '{"var22":"${n}"}'"
        /usr/local/bin/mage run magic-zoomcamp example_pipeline --runtime-vars '{"var22":"${n}"}'
        echo "miu"
        n=$(($n + 50)) 
    done