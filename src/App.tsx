import React from 'react';
import { OutlinerTree } from './components/OutlinerTree';
import { MainLayout } from './components/MainLayout';
import { PageTitle } from './components/PageTitle';
import { DevModeToolbar } from './components/DevModeToolbar';

function App() {
  // Get today's date formatted like LogSeq
  const today = new Date();
  const formattedDate = today.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  }).replace(',', 'th,');

  return (
    <>
      <MainLayout>
        <PageTitle
          title={formattedDate}
          tag="Journal"
          onAddIcon={() => console.log('Add icon')}
          onSetProperty={() => console.log('Set property')}
        />

        <OutlinerTree />
      </MainLayout>

      <DevModeToolbar />
    </>
  );
}

export default App;
