import axios from 'axios';

export default () => {
  return axios.create({
    baseURL: process.env.API_URL || 'http://localhost:3150/api/',
  });
};
