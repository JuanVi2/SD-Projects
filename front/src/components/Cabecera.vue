<template>

    <div id="header">
        <div id="logo">
            <h1>Practica Sistemas Distribuidos</h1>
        </div>

     
         <nav id="menu">
            <ul>
                <li id="register">
                    
                        <router-link to="/data">
                            <h2> 
                                DATOS
                            </h2>
                        </router-link>
                    
                </li>
                   
                <li id="login">
                    
                        <router-link to="/">
                            <h2>
                                MAP
                            </h2>
                        </router-link>
                    
                </li>
                
            </ul>
        </nav>
        
       
    </div>


</template>


<script>

export default{
    name: 'HeaderComponent'
}

</script>