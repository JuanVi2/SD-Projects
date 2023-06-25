<template>

    <div id="map">
      <component :is="dynamicComponentName"></component>
        <div id="map-mostrar">
          {{info.data}}          
       </div>
    </div>


</template>

<script>
import axios from "axios";


export default {
    data(){
      return{
        info: null,
        renderComponent: true
      }
    },
    mounted () {
    setTimeout(this.$forceUpdate(), 0.01)
    axios
      .get('http://localhost:5500/map')
      .then(response => (this.info = response))
  },

  
 



      
};

setTimeout(function(){
    location = ''
  },1000)

//setTimeout('document.location.reload()',5000);





</script>

<style scoped>
  @import '../assets/styles/map.css'; 
</style>
