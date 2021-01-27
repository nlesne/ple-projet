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
        <div v-if="val">
          <ul><li v-for="(name, val) in resultat" :key="name">
            <div> {{name}} : {{val}} </div>
          </li></ul>
        </div>
        <div v-else>
          <ul><li v-for="name in resultat" :key="name">
            <div> {{name}}</div>
          </li></ul>
        </div>
      </div>
    </div>
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
      val: false,
      begin: false,
      resultat: {
        type: Object,
      },
    };
  },
  components: {},
  methods: {
    onHastagTop: function() {
      this.begin = true;
      this.val = true;
    },
    onHastagTopKDay: function() {
      this.begin = true;
      this.val = true;

      const param = {
        day: this.day,
      };
      console.log(param);
    },
    onCountHastag: function() {
      this.begin = true;
      this.val = true;

      const param = {
        day: this.day,
        val: this.countHashtag,
      };
      console.log(param);
    },
    onUserForHashtag: function() {
      this.begin = true;
      this.val = false;

      const param = {
        day: this.day,
        val: this.usersForHashtag,
      };
      console.log(param);
    },
    onHastagUser: function() {
      this.begin = true;
      this.val = false;

      const param = {
        day: this.day,
        val: this.hashtagsForUser,
      };
      console.log(param);
    },
    onTweetUser: function() {
      this.begin = true;
      this.val = true;

      const param = {
        day: this.day,
        val: this.tweetForUser,
      };
      console.log(param);
    },
    onTweetCountry: function() {
      this.begin = true;
      this.val = true;

      const param = {
        day: this.day,
      };
      console.log(param);
    },
    onTweetLanguage: function() {
      this.begin = true;
      this.val = true;

      const param = {
        day: this.day,
      };
      console.log(param);
    },
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
