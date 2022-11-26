import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

	const getStats = () => {
	
        fetch(`http://api-lxvdev.westus3.cloudapp.azure.com/processing/stats`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
					<tbody>
                        <caption>Air Quality Readings</caption>
                        <tr>
                            <th colSpan="2">Temperature</th>
                        </tr>
						<tr>
							<td>Max: {stats['max_temp']}&deg;C</td>
							<td>Min: {stats['min_temp']}&deg;C</td>
						</tr>
						<tr>
							<td colspan="2">Average: {stats['avg_temp']}&deg;C</td>
						</tr>
                        <tr>
                            <th colSpan="2">Environment</th>
                        </tr>
						<tr>
							<td colspan="2">Max PM<sub>2.5</sub>: {stats['max_pm2_5']}ppm</td>
						</tr>
						<tr>
							<td colspan="2">Max CO<sub>2</sub>: {stats['max_co_2']}ppm</td>
						</tr>
					</tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>

            </div>
        )
    }
}
