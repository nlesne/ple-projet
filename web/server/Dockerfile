ARG port

FROM node:latest

WORKDIR /server

COPY package*.json ./

RUN npm install

EXPOSE port
CMD ["npm", "run", "start"]
