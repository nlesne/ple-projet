import Vue from 'vue';
import VueRouter from 'vue-router';
import Home from './views/Home.vue';
import ViewTweetUser from './views/ViewTweetUser';
import ViewTweetLanguage from './views/ViewTweetLanguage';
import ViewTweetCountByCountry from './views/ViewTweetCountByCountry';
import ViewhashtagsForUser from './views/ViewhashtagsForUser';
import ViewUsersWithHashtag from './views/ViewUsersWithHashtag';
import ViewcountHashtag from './views/ViewcountHashtag';
import ViewTopKHashtagsByDay from './views/ViewTopKHashtagsByDay';

Vue.use(VueRouter);

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home,
  },
  {
    path: '/ViewTweetUser',
    name: 'ViewTweetUser',
    component: ViewTweetUser,
  },
  {
    path: '/ViewTweetLanguage',
    name: 'ViewTweetLanguage',
    component: ViewTweetLanguage,
  },
  {
    path: '/ViewTweetCountByCountry',
    name: 'ViewTweetCountByCountry',
    component: ViewTweetCountByCountry,
  },
  {
    path: '/ViewhashtagsForUser',
    name: 'ViewhashtagsForUser',
    component: ViewhashtagsForUser,
  },
  {
    path: '/ViewUsersWithHashtag',
    name: 'ViewUsersWithHashtag',
    component: ViewUsersWithHashtag,
  },
  {
    path: '/ViewcountHashtag',
    name: 'ViewcountHashtag',
    component: ViewcountHashtag,
  },
  {
    path: '/ViewTopKHashtagsByDay',
    name: 'ViewTopKHashtagsByDay',
    component: ViewTopKHashtagsByDay,
  },
];

const router = new VueRouter({
  mode: 'history',
  base: process.env.BASE_URL,
  routes,
});

export default router;
