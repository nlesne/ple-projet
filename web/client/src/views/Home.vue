<template>
  <div class="Home">
    <b-sidebar
      :fullheight="true"
      :overlay="false"
      open
      :can-cancel="false">
      <div class="title">Tri : </div>
      <b-field label="Jour:">
        <b-input v-model="day"></b-input>
      </b-field>
      <div class="subtitle"> Hastags : </div>

      <b-button type="is-primary is-info" outlined
        v-on:click="onHastagTopK"> Top k </b-button>

      <b-button type="is-primary is-light" outlined
        v-on:click="onHastagTopK"> Top k par jour</b-button>

      <b-field label="Nombre d'utilisation d'un hashtag:">
        <b-input v-model="countHashtag"></b-input>
      </b-field>
       <b-button type="is-primary is-info" outlined
       v-on:click="onCountHastag"> valide</b-button>

      <b-field label="Liste d'utilisateur pour un hashtag:">
        <b-input v-model="usersForHashtag"></b-input>
      </b-field>
      <b-button type="is-primary is-light" outlined
        v-on:click="onUserForHashtag"> valide</b-button>

      <div class="subtitle"> Users : </div>

      <b-button type="is-primary is-info" outlined
        v-on:click="onTweetCountry">Nombre de Tweet par pays</b-button>

      <b-button type="is-primary is-light" outlined
        v-on:click="onTweetLanguage"> Nombre de Tweet par langue</b-button>

      <b-field label="Nombre d'Hashtag d'un utilisateur:">
        <b-input v-model="hashtagsForUser"></b-input>
      </b-field>
      <b-button type="is-primary is-info" outlined
        v-on:click="onHastagUser"> valide</b-button>

      <b-field label="Nombre de Tweet d'un utilisateur:">
        <b-input v-model="tweetForUser"></b-input>
      </b-field>
      <b-button type="is-primary is-light" outlined
        v-on:click="onTweetUser">valide </b-button>

    </b-sidebar>
    <div class="body">
      <div class="title">Résultat : </div>
      <div v-if="begin">
      <div v-for="val in resultat" v-bind:key="val">
        {{val}}
      </div>
      </div>
    </div>
  </div>
</template>

<script>
import UsersService from '../services/Users';
import HastagsService from '../services/Hashtags';
export default {
  name: '',
  props: {},
  data() {
    return {
      day: 1,
      countHashtag: '',
      usersForHashtag: '',
      hashtagsForUser: '',
      tweetForUser: '',
      begin: false,
      resultat: {
        type: Array,
      },
    };
  },
  components: {},
  methods: {

    // //HASHTAG///////
    onHastagTop: function() {
      HastagsService.getTopKHashtagsByDay(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onHastagTopKDay: function() {
      HastagsService.getTopKHashtagsByDay(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onCountHastag: function() {
      HastagsService.getHashtagsCount(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onUserForHashtag: function() {
      HastagsService.getUsersWithHashtag(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },

    // ////USERS//////
    onHastagUser: function() {
      UsersService.getUsersHashtags(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onTweetUser: function() {
      UsersService.getTweetCountByLang(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onTweetCountry: function() {
      UsersService.getTweetCountByCountry(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
    // ********************
    onTweetLanguage: function() {
      UsersService.getTweetCountByLang(this.day)
          .then((resp) => {
            this.begin = true;
            this.resultat = resp.data.result;
            console.log(resp.data.result);
          })
          .catch((err) => console.error(err));
    },
  },
  mounted: function() {
  },
};
</script>

<style scoped>
.body{
  margin-left: 270px;
}
li {
  list-style-type: square;
  margin-left: 50px;
}
</style>
