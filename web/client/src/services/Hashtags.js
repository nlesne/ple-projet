import api from '@/services/api';

export default {

    getHashtagsCount(day) {
      return api().get('hashtags/count', {params: {day : day}});
    },
  
    getUsersWithHashtag(day) {
      return api().get('hashtags/users', {params: {day : day}});
    },
  
    getTopKHashtagsByDay(day) {
      return api().get('hashtags/topk-day', {params: {day : day}});
    },
  };