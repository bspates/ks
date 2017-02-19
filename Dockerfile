FROM node:wheezy
WORKDIR /srv/app
COPY ./package.json /srv/app
RUN npm install -q
CMD npm test
