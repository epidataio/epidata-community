"use strict"

// gets route from html document
const authenticateRoute = document.getElementById("authenticateRoute").value;
// calls authenticate when submitbutton is pressed
document.getElementById("submitbutton").addEventListener("click", authenticate);

function authenticate() {
    // retrieves id and token from document
    const device_id = document.getElementById("device_id").value;
    const device_token = document.getElementById("device_token").value;
    // sends a request to authentication route to see if device should be authenticated
    fetch(authenticateRoute,{
        method:'POST',
        header:{'Content-type': 'application/json', 'device_id': device_id, 'device_token': device_token},
        body:JSON.stringify({device_id, device_token})
    }).then(res=> res.json()).then(data => {
        if(data){
            console.log(data)
            if (data.status == "authenticated") {
              document.getElementById("authstatus").innerHTML = "Authentication Successful!!"
              document.getElementById("authstatus").style.display = "block"
              document.getElementById("device_token").value = ""
            }
            else {
              document.getElementById("authstatus").innerHTML = "Authentication Failed. Please Try Again."
              document.getElementById("authstatus").style.display = "block"
              document.getElementById("device_token").value = ""
            }
        }
    })
}
