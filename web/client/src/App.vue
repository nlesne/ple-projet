<template>
  <div id="app">
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
        v-on:click="onHastagTop"> Top k </b-button>

      <b-button type="is-primary is-light" outlined
        v-on:click="onHastagTopKDay"> Top k par jour</b-button>

      <b-field label="Nombre d'utilisation d'un hashtag:">
        <b-input v-model="countHashtag"></b-input>
      </b-field>
       <b-button type="is-primary is-info" outlined
       v-on:click="onCountHastag"> valide</b-button>

      <b-button type="is-primary is-light" outlined
        v-on:click="onUserForHashtag">
        Utilisateurs ayant utilisé un hashtag
        </b-button>

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
    <router-view>
      :day="day"
    </router-view>
  </div>
</template>

<script>
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
      this.$router.push('/ViewTopKHashtags');
    },
    // ********************
    onHastagTopKDay: function() {
      this.$router.push('/ViewTopKHashtagsByDay');
    },
    // ********************
    onCountHastag: function() {
      this.$router.push('/ViewcountHashtag');
    },
    // ********************
    onUserForHashtag: function() {
      this.$router.push('/ViewUsersWithHashtag');
    },

    // ////USERS//////
    onHastagUser: function() {
      this.$router.push('/ViewhashtagsForUser');
    },
    // ********************
    onTweetUser: function() {
      this.$router.push('/ViewTweetUser');
    },
    // ********************
    onTweetCountry: function() {
      this.$router.push('/ViewTweetCountByCountry');
    },
    // ********************
    onTweetLanguage: function() {
      this.$router.push('/ViewTweetLanguage');
    },
  },
};
</script>

<style>

</style>
