<template>
  <div class="Home">
    <b-sidebar
      :fullheight="true"
      :overlay="false"
      open
      :can-cancel="false">
      <div class="title">Tri : </div>
      <div class="subtitle"> Hastags : </div>
      <b-button type="is-primary is-light" outlined
        v-on:click="onHastagTopK"> Top k </b-button>
      <b-button type="is-primary is-info" outlined
        v-on:click="onCountHastag"> Nombre d'utilisation d'un hashtag</b-button>
      <b-button type="is-primary is-light" outlined
        v-on:click="onUserForHashtag">
          Liste d'utilisateur pour un hashtag</b-button>
      <div class="subtitle"> Users : </div>
      <b-button type="is-primary is-info" outlined
        v-on:click="onHastagUser"> Nombre d'Hashtag d'un utilisateur</b-button>
      <b-button type="is-primary is-light" outlined
        v-on:click="onTweetUser"> Nombre de Tweet d'un utilisateur</b-button>
      <b-button type="is-primary is-info" outlined
        v-on:click="onTweetCountry">Nombre de Tweet par pays</b-button>
      <b-button type="is-primary is-light" outlined
        v-on:click="onTweetLanguage"> Nombre de Tweet par langue</b-button>
    </b-sidebar>
    <div class="body">
      <div class="title">Résultat : </div>
      <ul><li v-for="ele in resultat" :key="ele">
        <div v-if="val"> {{ele.name}} : {{ele.val}} </div>
        <div v-else>  {{ele.name}} </div>
      </li></ul>
    </div>
  </div>
</template>

<script>
export default {
  name: '',
  props: {},
  data() {
    return {
      val: true,
      resultat: {
        type: Object,
      },
    };
  },
  components: {},
  methods: {
    modifyBody: function(res) {
      console.log(res);
      const body = document.querySelector('#resultat');
      body.innerHTML = '';
      const text = document.createElement('div');
      body.appendChild(text);
      text.innerHTML = res;
    },
    onHastagTopK: function() {
      this.modifyBody('Top K !');
    },
    onCountHastag: function() {
      this.modifyBody('Compte un hashtag!');
    },
    onUserForHashtag: function() {
      this.modifyBody('Liste dutilisateur pour un hashtag!');
    },
    onHastagUser: function() {
      this.modifyBody('Nombre d hashtag pour un utilisateur!');
    },
    onTweetUser: function() {
      this.modifyBody('Nombre de tweet pour un utilisateur!');
    },
    onTweetCountry: function() {
      this.modifyBody('Nombre de tweet par pays');
    },
    onTweetLanguage: function() {
      this.modifyBody('Nombre de tweet par langue');
    },
  },
};
</script>

<style scoped>
.body{
  margin-left: 270px;
}
</style>
