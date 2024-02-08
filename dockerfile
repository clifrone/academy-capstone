FROM public.ecr.aws/datamindedacademy/capstone:v3.4.1-hadoop-3.3.6-v1

USER 0

COPY containerize/requirements.txt .

RUN pip install -r requirements.txt 

WORKDIR /spark_aws_to_snowflake 

COPY spark_aws_to_snowflake/* /spark_aws_to_snowflake/


ENV AWS_ACCESS_KEY_ID = ""
ENV AWS_SECRET_ACCESS_KEY =""
ENV AWS_DEFAULT_REGION = ""

#ENTRYPOINT [ "/bin/bash" ]

ENTRYPOINT [ "python3" ]
CMD ["load_data_from_aws.py"]