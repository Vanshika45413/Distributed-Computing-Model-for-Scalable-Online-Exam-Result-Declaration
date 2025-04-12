// src/App.tsx
import React, { useState, useEffect, useCallback, FormEvent, KeyboardEvent } from 'react';
import { Activity, Search, Server, Users, Clock, AlertTriangle, Database, HeartPulse } from 'lucide-react'; // Added Database, HeartPulse

// Interface for Zone data from backend API
interface ZoneStatus {
  name: string;
  status: 'healthy' | 'medium-load' | 'high-load'; // Adjusted status type
  load: number; // This is now percentage
  dataCenterId: string;
  clusterCount: number;
  serversPerCluster: number;
  replicaCount: number;
  currentLeader?: string; // Optional: Add leader info
  downReplicas?: number; // Optional: Add down count
}

// Interface for the overall status API response
interface SystemStatusResponse {
    zones: ZoneStatus[];
    activeUsers: number;
    responseTime: number;
    incidents: number;
}


// Interface for Search API response/error data (keep as before)
interface ApiSuccessData { rollNumber: string; result: string; }
interface ApiErrorData { rollNumber?: string; error: string; }

// --- NO LONGER NEED INITIAL_ZONES for dynamic data ---
// const INITIAL_ZONES: ZoneStatus[] = [ ... ];

function App() {
  // Page state
  const [currentPage, setCurrentPage] = useState<'search' | 'status'>('search');

  // Search Page State
  const [rollNumber, setRollNumber] = useState('');
  const [searchResult, setSearchResult] = useState<string | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searchIsLoading, setSearchIsLoading] = useState(false);
  const [lastSubmittedRoll, setLastSubmittedRoll] = useState('');

  // Status Page State (will be fetched)
  const [zones, setZones] = useState<ZoneStatus[]>([]); // Start empty
  const [activeUsers, setActiveUsers] = useState<number | null>(null); // Start null
  const [responseTime, setResponseTime] = useState<number | null>(null); // Start null
  const [incidents, setIncidents] = useState<number | null>(null); // Start null
  const [statusError, setStatusError] = useState<string | null>(null); // Error fetching status
  const [statusIsLoading, setStatusIsLoading] = useState<boolean>(true); // Loading initially

  // --- Fetch System Status Periodically ---
  const fetchSystemStatus = useCallback(async () => {
     // Don't need to set loading true on every poll, only initially
     // setStatusIsLoading(true);
     setStatusError(null); // Clear previous error
     const statusApiUrl = 'http://localhost:5000/api/status';
     try {
         const response = await fetch(statusApiUrl);
         if (!response.ok) {
             throw new Error(`Failed to fetch status: ${response.status} ${response.statusText}`);
         }
         const data: SystemStatusResponse = await response.json();
         // Update state with data from backend
         setZones(data.zones);
         setActiveUsers(data.activeUsers);
         setResponseTime(data.responseTime);
         setIncidents(data.incidents);
     } catch (err) {
         console.error("Error fetching system status:", err);
         if (err instanceof Error) {
            setStatusError(err.message);
         } else {
             setStatusError("An unknown error occurred fetching status.");
         }
         // Optionally clear old data on error or leave it stale
         // setZones([]); setActiveUsers(null); ...
     } finally {
         // Set loading false only after the *first* successful fetch or error
         if(statusIsLoading) setStatusIsLoading(false);
     }
  }, [statusIsLoading]); // Include statusIsLoading dependency

  useEffect(() => {
    // Fetch status immediately on component mount
    fetchSystemStatus();

    // Set up interval to fetch status every 5 seconds
    const intervalId = setInterval(fetchSystemStatus, 5000);

    // Cleanup function to clear interval when component unmounts
    return () => clearInterval(intervalId);
  }, [fetchSystemStatus]); // Depend on the fetchSystemStatus callback

  // --- Function to handle API call for Search Page ---
  const handleFetchResult = useCallback(async () => {
      const trimmedRollNumber = rollNumber.trim();
      if (!trimmedRollNumber) {
        setSearchError('Please enter a roll number.');
        setSearchResult(null); setLastSubmittedRoll(''); return;
      }
      setSearchIsLoading(true); setSearchError(null); setSearchResult(null);
      setLastSubmittedRoll(trimmedRollNumber);
      const apiUrl = `http://localhost:5000/api/result/${encodeURIComponent(trimmedRollNumber)}`;
      try {
        const response = await fetch(apiUrl, { method: 'GET' });
        let responseData: ApiSuccessData | ApiErrorData | null = null;
        let responseText = '';
        try { responseText = await response.text(); responseData = JSON.parse(responseText); }
        catch (jsonError) {
             console.error("Failed to parse JSON:", jsonError, "| Response:", responseText);
             if (response.ok) throw new Error(`Unexpected non-JSON: ${responseText.substring(0,100)}`);
        }
        if (!response.ok) {
            if (response.status === 404 && responseData && 'result' in responseData && responseData.result === "Result Not Found") {
                 setSearchResult(responseData.result); setSearchError(null);
            } else {
                 const errorMsg = responseData && 'error' in responseData ? responseData.error : `Request failed: ${response.status} ${response.statusText}`;
                 throw new Error(errorMsg);
            }
        } else {
             setSearchResult((responseData as ApiSuccessData).result); setSearchError(null);
        }
      } catch (err) {
          if (err instanceof Error) { setSearchError(err.message); console.error("Fetch error:", err.message); }
          else { const unknownError = 'Unknown fetch error.'; setSearchError(unknownError); console.error(unknownError, err); }
          setSearchResult(null);
      } finally {
        setSearchIsLoading(false);
      }
  }, [rollNumber]);

  // Handle form submission for search
  const handleSubmit = async (e: FormEvent) => { e.preventDefault(); await handleFetchResult(); };
  // Handle enter key for search
  const handleKeyPress = (event: KeyboardEvent<HTMLInputElement>) => { if (event.key === 'Enter' && !searchIsLoading) { handleFetchResult(); } };

  // --- Helper functions for Status Page styling ---
  const getLoadColor = (load: number) => {
    if (load < 50) return 'bg-green-500';
    if (load < 80) return 'bg-yellow-500'; // Changed threshold
    return 'bg-red-500';
  };
  const getStatusBadge = (status: ZoneStatus['status']) => {
    const base = "px-2 py-1 rounded-full text-xs font-medium";
    if (status === 'healthy') return `${base} bg-green-100 text-green-800`;
    if (status === 'medium-load') return `${base} bg-yellow-100 text-yellow-800`; // Added medium
    return `${base} bg-red-100 text-red-800`; // Was yellow before
  };

  // --- JSX Rendering ---
  return (
    <div className="min-h-screen bg-gray-100"> {/* Changed background */}
      {/* Header */}
      <header className="bg-white border-b sticky top-0 z-10 shadow-sm"> {/* Added shadow */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center">
              <Activity className="h-6 w-6 text-indigo-600" />
              <h1 className="ml-2 text-xl font-semibold text-gray-900">Distributed System Portal</h1>
            </div>
            <div className="flex items-center space-x-1 sm:space-x-4">
              <button onClick={() => setCurrentPage('search')} className={`px-3 sm:px-4 py-2 text-sm sm:text-base font-medium rounded-md ${currentPage === 'search' ? 'bg-indigo-100 text-indigo-700' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-700'}`}> Result Search </button>
              <button onClick={() => setCurrentPage('status')} className={`px-3 sm:px-4 py-2 text-sm sm:text-base font-medium rounded-md ${currentPage === 'status' ? 'bg-indigo-100 text-indigo-700' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-700'}`}> System Status </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 sm:py-12">
        {currentPage === 'search' ? (
          // --- Search Page ---
          <div className="max-w-2xl mx-auto">
            {/* (Search form and result display - kept mostly the same logic as before) */}
             <div className="bg-white rounded-lg shadow-md p-6 sm:p-8">
              <h2 className="text-2xl font-semibold text-gray-900 mb-6 text-center">Check Your Results</h2>
              <form onSubmit={handleSubmit}>
                 {/* ... (Input and button same as before, using searchIsLoading) ... */}
                 <div className="space-y-4">
                  <div>
                    <label htmlFor="rollNumber" className="sr-only">Enter Roll Number</label>
                    <div className="relative">
                      <input type="text" id="rollNumber" value={rollNumber} onChange={(e) => setRollNumber(e.target.value)} onKeyPress={handleKeyPress} placeholder="Enter Roll Number (e.g., R0000)" className="block w-full pl-4 pr-10 py-3 text-base rounded-lg border border-gray-300 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 shadow-sm" disabled={searchIsLoading} aria-label="Roll Number Input" />
                      <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none"><Search className="h-5 w-5 text-gray-400" /></div>
                    </div>
                  </div>
                  <button type="submit" disabled={searchIsLoading || !rollNumber.trim()} className={`w-full flex justify-center items-center bg-indigo-600 text-white py-3 px-4 border border-transparent rounded-lg shadow-sm text-base font-medium hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition duration-150 ease-in-out ${(searchIsLoading || !rollNumber.trim()) ? 'opacity-50 cursor-not-allowed' : ''}`}>
                    {searchIsLoading ? (<><svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg>Loading...</>) : 'View Result'}
                  </button>
                </div>
              </form>
              {/* Display Area */}
              <div className="mt-6 min-h-[80px]">
                  {searchError && ( <div className="bg-red-100 border-l-4 border-red-500 text-red-700 p-4 rounded-md" role="alert"><p className="font-bold">Error</p><p>Could not retrieve result for <span className='font-medium'>{lastSubmittedRoll}</span>: {searchError}</p></div>)}
                  {searchResult && ( <div className={`border-l-4 p-4 rounded-md ${searchResult === "Result Not Found" ? 'bg-yellow-100 border-yellow-500 text-yellow-700' : 'bg-green-100 border-green-500 text-green-700'}`} role="alert"><p className="font-bold">{searchResult === "Result Not Found" ? "Information" : "Success"}</p><p>Result for <span className='font-medium'>{lastSubmittedRoll}</span>: <span className="font-semibold">{searchResult}</span></p></div>)}
                   {searchIsLoading && ( <p className="text-gray-500 text-center p-4">Fetching result...</p> )}
                   {!searchIsLoading && !searchError && !searchResult && !lastSubmittedRoll && ( <p className="text-gray-500 text-center italic p-4">Enter a roll number.</p> )}
              </div>
            </div>
          </div>
        ) : (
          // --- Status Page ---
          <div>
            <h2 className="text-2xl font-semibold text-gray-900 mb-6">System Overview</h2>
            {/* Show loading indicator or error for status */}
            {statusIsLoading && <p className="text-center text-gray-600">Loading system status...</p>}
            {statusError && <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert"> Status Update Error: {statusError} </div>}

            {!statusIsLoading && !statusError && (
                <>
                    {/* Zones Status */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                        {zones.map((zone) => (
                            <div key={zone.name} className="bg-white rounded-lg shadow-md p-6 border border-gray-200 hover:shadow-lg transition-shadow">
                                <div className="flex items-center justify-between mb-4">
                                    <div className="flex items-center"><Server className="h-5 w-5 text-gray-500 mr-2" /><h3 className="text-lg font-medium text-gray-900">{zone.name}</h3></div>
                                    <span className={getStatusBadge(zone.status)}>{zone.status.replace('-', ' ')}</span> {/* Nicer display */}
                                </div>
                                <div className="space-y-4">
                                    <div>
                                        <div className="flex justify-between text-sm mb-1"><span className="text-gray-500">Avg Server Load</span><span className="font-medium">{zone.load}%</span></div>
                                        <div className="h-2 bg-gray-200 rounded-full overflow-hidden"><div className={`h-full transition-all duration-500 ease-in-out ${getLoadColor(zone.load)}`} style={{ width: `${zone.load}%` }}/></div>
                                    </div>
                                    <div className="text-sm text-gray-600 border-t pt-3 mt-3 space-y-1">
                                        <p><strong className="text-gray-700">Data Center:</strong> {zone.dataCenterId}</p>
                                        <p><strong className="text-gray-700">Clusters:</strong> {zone.clusterCount} ({zone.clusterCount * zone.serversPerCluster} Servers Total)</p>
                                        {/* <p>Servers/Cluster: {zone.serversPerCluster}</p> */}
                                        <p className="flex items-center"><Database size={14} className="mr-1 text-gray-500"/> <strong className="text-gray-700">DB Replicas:</strong> {zone.replicaCount}</p>
                                        {zone.currentLeader && <p><strong className="text-gray-700">DB Leader:</strong> {zone.currentLeader}</p>}
                                        {typeof zone.downReplicas === 'number' && zone.downReplicas > 0 && (
                                            <p className="flex items-center text-red-600"><HeartPulse size={14} className="mr-1"/> <strong className="text-red-700">Down Replicas:</strong> {zone.downReplicas}</p>
                                        )}
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Statistics */}
                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
                        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
                           <div className="flex items-center space-x-3 text-gray-500 mb-3"><Users className="h-6 w-6" /><h3 className="text-lg font-medium">Active Connections</h3></div> {/* Changed label slightly */}
                           <p className="text-4xl font-bold text-indigo-600">{activeUsers?.toLocaleString() ?? 'N/A'}</p><p className="text-sm text-gray-500 mt-1">Estimated from server load</p>
                        </div>
                        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
                            <div className="flex items-center space-x-3 text-gray-500 mb-3"><Clock className="h-6 w-6" /><h3 className="text-lg font-medium">Response Time</h3></div>
                            <p className="text-4xl font-bold text-green-600">{responseTime ?? 'N/A'}ms</p><p className="text-sm text-gray-500 mt-1">Estimated server response</p>
                        </div>
                        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
                            <div className="flex items-center space-x-3 text-gray-500 mb-3"><AlertTriangle className="h-6 w-6" /><h3 className="text-lg font-medium">Incidents</h3></div>
                            <p className="text-4xl font-bold text-yellow-600">{incidents ?? 'N/A'}</p><p className="text-sm text-gray-500 mt-1">Down database replicas</p>
                        </div>
                    </div>
                </>
            )}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;