FROM python

ADD . /usr/src/mercury-api
RUN pip install -r /usr/src/mercury-api/requirements.txt
RUN cd /usr/src/mercury-api && pip install -e .

CMD mercury-api

