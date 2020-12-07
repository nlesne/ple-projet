FROM node:latest

WORKDIR /client

COPY package*.json ./

RUN npm install

EXPOSE 8080

CMD [ "npm", "run", "serve" ]