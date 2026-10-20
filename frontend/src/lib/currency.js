import { createContext, useContext } from 'react';

export const CurrencyRegionContext = createContext('');

export function formatCurrency(value, region = '') {
  const prefix = region === 'cn-north-1' || region === 'cn-northwest-1' ? 'CN¥' : '$';
  return `${prefix}${Number(value || 0).toFixed(2)}`;
}

export function useCurrencyFormatter() {
  const region = useContext(CurrencyRegionContext);
  return (value) => formatCurrency(value, region);
}