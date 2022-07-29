"use strict"

const authenticateRoute = document.getElementById("authenticateRoute").value;
document.getElementById("submitbutton").addEventListener("click", authenticate);


function authenticate(){
            const device_id = document.getElementById("device_id").value;
            const device_token = document.getElementById("device_token").value;
            fetch(authenticateRoute,{
                method:'POST',
                header:{'Content-type': 'application/json', 'device_id': device_id, 'device_token': device_token},
                body:JSON.stringify({device_id, device_token})
            }).then(res=> res.json()).then(data => {
                if(data){
                    console.log(data)
                }
            })
        }



