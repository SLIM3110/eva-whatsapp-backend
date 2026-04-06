export const toUAETime = (dateStr: string | null): string => {
  if (!dateStr) return '';
  const date = new Date(dateStr);
  return date.toLocaleString('en-AE', { timeZone: 'Asia/Dubai' });
};

export const isToday = (dateStr: string | null): boolean => {
  if (!dateStr) return false;
  const now = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });
  const d = new Date(dateStr).toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });
  return now === d;
};

export const isThisMonth = (dateStr: string | null): boolean => {
  if (!dateStr) return false;
  const now = new Date();
  const d = new Date(dateStr);
  const nowMonth = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Dubai' }));
  const dMonth = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Dubai' }));
  return nowMonth.getFullYear() === dMonth.getFullYear() && nowMonth.getMonth() === dMonth.getMonth();
};

export const getTodayStartUTC = (): string => {
  const now = new Date();
  const uaeDate = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Dubai' }));
  uaeDate.setHours(0, 0, 0, 0);
  const offset = 4 * 60 * 60 * 1000;
  return new Date(uaeDate.getTime() - offset).toISOString();
};
