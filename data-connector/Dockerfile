FROM node:alpine as base

WORKDIR /data-connector

COPY package.json package-lock.json ./

RUN rm -rf node_modules && npm install

COPY . .

CMD ["node", "./app.js"]