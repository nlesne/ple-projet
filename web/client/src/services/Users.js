import api from '@/services/api';

export default {

  getUsersTweetCount(day) {
    return api().get('users/count', {params: {day : day}});
  },

  getUsersHashtags(day) {
    return api().get('users/hashtags', {params: {day : day}});
  },

  getTweetCountByCountry(day) {
    return api().get('users/country', {params: {day : day}});
  },

  getTweetCountByLang(day) {
    return api().get('users/lang', {params: {day : day}});
  },
};