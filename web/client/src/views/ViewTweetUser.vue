<template>
  <div class="Home">
    <div class="body">
      <div class="title">Résultat : </div>
        {{resultat}}
    </div>
  </div>
</template>

<script>
import Users from '../services/Users';
export default {
  name: '',
  props: {},
  data() {
    return {
      resultat: '',
    };
  },
  components: {},
  methods: {},
  // , 'psyblogueuse' Hashtags  Users
  mounted: function() {
    Users.getUserTweetCount(1, 'psyblogueuse')
        .then((resp) => {
          this.begin = true;
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
