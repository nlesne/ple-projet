<template>
  <div class="Home">
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
import Hashtags from '../services/Hashtags';
export default {
  name: '',
  props: {},
  data() {
    return {
      resultat: {
        type: Array,
      },
    };
  },
  components: {},
  methods: {},
  // , 'psyblogueuse' Hashtags  Users
  mounted: function() {
    Hashtags.getTopKHashtagsByDay(this.day)
        .then((resp) => {
          this.resultat = resp.data.result;
          console.log(resp.data.result);
        })
        .catch((err) => console.error(err));
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
