import api from '@/services/api';

export default {

  getHashtagCount(day, hashtag) {
    return api().get('hashtags/count', {params: {day: day, hashtag: hashtag}});
  },

  getUsersWithHashtag(day, hashtag) {
    return api().get('hashtags/users', {params: {day: day, hashtag: hashtag}});
  },

  getTopKHashtagsByDay(day) {
    return api().get('hashtags/topk-day', {params: {day: day}});
  },

  getTopKHashtags(day) {
    return api().get('hashtags/topk');
  },
};
