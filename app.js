let ticketBooking = new Promise((resolve,reject)=>
{
    ticketBooked = true
    if(ticketBooked)
    {
        resolve()
    }
    else
    {
        reject()
    }
})

ticketBooking.then(()=>console.log("ticket booked successfully")).catch(()=>console.log("ticket is not available"))