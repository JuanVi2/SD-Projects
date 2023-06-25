<template>
  <div class="map-container">
    <h2 class="legend">Leyenda:</h2>
    <ul class="legend-list">
      <li>M - Mina</li>
      <li>A - Alimento</li>
      <li>Número - NPC</li>
      <li>Letra minúscula - Jugador</li>
    </ul>
    <table class="map-table">
      <tr v-for="(row, rowIndex) in map" :key="rowIndex">
        <td v-for="(cell, cellIndex) in row" :key="cellIndex" :class="getCellClass(cell)">{{ cell }}</td>
      </tr>
    </table>
  </div>
</template>

<script>
import axios from 'axios';


export default {
  data() {
    return {
      map: []
    };
  },
  mounted() {
    this.fetchMap();
    setTimeout(this.$forceUpdate(), 5.00);
  },
  methods: {
    fetchMap() {
      axios.get('https://192.168.100.244:443/map')
        .then(response => {
          this.map = response.data;
        })
        .catch(error => {
          console.error(error);
        });
    },
    getCellClass(cell) {
      return {
        'cell-red': cell === 'M',
        'cell-green': cell === 'A',
        'cell-blue': /^[a-z]$/.test(cell),
        'cell-orange': /^-?\d+(\.\d+)?$/.test(cell)
      };
    }
  }
};

setTimeout(function(){
    location = ''
  },1000)

</script>

<style scoped>
.map-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  width: 50%;
  margin: 0 auto;
}

.legend {
  font-size: 1.2rem;
  margin-top: 20px;
  text-align: center;
}

.legend-list {
  list-style-type: none;
  padding: 0;
  margin: 10px 0;
}

.legend-list li {
  margin-bottom: 5px;
}
.map-table {
  border-collapse: collapse;
  table-layout: fixed;
  width: 100%;
  height: 100%;
  border: 2px solid black;
  background-color: rgb(143, 143, 143);
}

.map-table td {
  border: 1px solid #ccc;
  padding: 8px;
  text-align: center;
  white-space: nowrap;
  height: 40px;
  box-sizing: border-box;
}

.map-table td.border-top {
  border-top-width: 2px;
  border-top-color: black;
}

.map-table td.border-left {
  border-left-width: 2px;
  border-left-color: black;
}

.cell-red {
  background-color: red;
}

.cell-green {
  background-color: rgb(21, 209, 21);
}

.cell-blue {
  background-color: rgb(0, 166, 255);
}

.cell-orange {
  background-color: orange;
}
</style>
