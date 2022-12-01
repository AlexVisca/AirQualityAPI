import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStatus() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [status, setStatus] = useState({});
    const [error, setError] = useState(null)

	const getStatus = () => {
	
        fetch(`http://api-lxvdev.westus3.cloudapp.azure.com/healthcheck/health`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Status")
                setStatus(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStatus(), 10000); // Update every 10 seconds
		return() => clearInterval(interval);
    }, [getStatus]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <table className={"StatusTable"}>
					<tbody>
                        <tr>
                            <th colSpan="2">System Status</th>
                        </tr>
						<tr>
							<td colspan="2">System: {status['system']}</td>
						</tr>
						<tr>
							<td colSpan="2">Receiver: {status['receiver']}</td>
						</tr>
                        <tr>
                            <td colSpan="2">Storage: {status['storage']}</td>
                        </tr>
						<tr>
							<td colspan="2">Processing: {status['processing']}</td>
						</tr>
						<tr>
							<td colspan="2">Audit Log: {status['auditlog']}</td>
						</tr>
					</tbody>
                </table>
                <h3>Last Updated: {status['last_updated']}</h3>
            </div>
        )
    }
}
