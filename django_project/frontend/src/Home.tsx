import React from 'react';
import './styles/App.scss';
import { useGapContext } from './contexts/GapContext';

function Home() {
  const gapContext = useGapContext()

  const redirectToURL = (url: string) => {
    window.location.href = url
  }

  return (
    <div className="App">
      <header className="App-header">
        <p>
          OSIRIS II Global Access Platform
        </p>
        <div className='button-container'>
          <div
            className="App-link link-button" onClick={(e) => redirectToURL(gapContext.api_swagger_url)}
          >
            API Swagger Docs
          </div>
          <div
            className="App-link link-button" onClick={(e) => redirectToURL(gapContext.api_docs_url)}
          >
            API Documentation
          </div>
        </div>        
      </header>
    </div>
  );
}

export default Home;
