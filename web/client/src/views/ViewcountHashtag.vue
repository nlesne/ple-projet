<template>
  <div class="Home">
    <div class="body">
      <div class="title">Résultat : </div>
        {{this.val}}
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
      val: '',
    };
  },
  components: {},
  methods: {},
  // , 'psyblogueuse' Hashtags  Users
  mounted: function() {
    Hashtags.getHashtagCount(1, 'coronavirus')
        .then((resp) => {
          this.begin = true;
          this.val = resp.data.result;
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
