export const API = (window as any).__API_URL__ || (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000';
export const WS = API.replace('http', 'ws');
