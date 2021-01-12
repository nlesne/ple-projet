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
      <div class="subtitle"> Users : </div>
      <div class="subtitle"> influenceurs : </div>
    </b-sidebar>
    <div class="body">
      <div class="title">Résultat : </div>
      <div id="resultat">hola</div>
    </div>
  </div>
</template>

<script>
export default {
  name: '',
  props: {},
  data() {},
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
      this.modifyBody('Top K K !');
    },
  },
};
</script>

<style scoped>
.body{
  margin-left: 270px;
}
</style>
