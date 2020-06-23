FROM 292075781285.dkr.ecr.us-east-1.amazonaws.com/indra_db:latest
ENV EIDOSPATH /sw/eidos-assembly-0.2.3-SNAPSHOT.jar
RUN rm -r /sw/reach && \
    rm -r /sw/sparser && \
    rm bionetgen.tar.gz && \
    rm -r BioNetGen*
ADD eidos-assembly-0.2.3-SNAPSHOT.jar $EIDOSPATH
