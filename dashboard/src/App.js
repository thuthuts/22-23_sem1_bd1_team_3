import './App.scss';
import {Routes, Route} from 'react-router-dom';
//import HomeContainer from './components/containers/HomeContainer';
import * as Constants from './Constants';
//import CompanyNewsContainer from './components/containers/CompanyNewsContainer';
import Top from './components/navbar/Top';
import { CompanySelectorContext } from './components/contexts/CompanySelectorContext';
import {useState} from 'react'
import SocialMediaContainer from './components/containers/SocialMediaContainer';

function App() {
  const [companySelector, setCompanySelector] = useState(null);
  return (
  <>
    <CompanySelectorContext.Provider value={[companySelector, setCompanySelector]}>
      <Top />
        <Routes>
          <Route path={Constants.HOME_ROUTE} exact element={HomeContainer}/>
          <Route path={Constants.COMPANY_NEWS_ROUTE} exact element={CompanyNewsContainer}/>
          <Route path={Constants.SOCIAL_MEDIA_ROUTE} exact element={SocialMediaContainer}/>
        </Routes>
    </CompanySelectorContext.Provider>
  </>
  );
}

export default App;
