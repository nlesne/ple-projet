FROM node:latest

WORKDIR /server

COPY package*.json ./

RUN npm install

EXPOSE 3000
CMD ["npm", "run", "start"]
