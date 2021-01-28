import api from '@/services/api';

export default {

  getUserTweetCount(day, user) {
    return api().get('users/count', {params: {day : day, user: user}});
  },

  getUserHashtags(day, user) {
    return api().get('users/hashtags', {params: {day : day, user: user}});
  },

  getTweetCountByCountry(day) {
    return api().get('users/country', {params: {day : day}});
  },

  getTweetCountByLang(day) {
    return api().get('users/lang', {params: {day : day}});
  },
};