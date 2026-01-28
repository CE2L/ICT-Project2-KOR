'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';

export default function Dashboard() {
  const [artists, setArtists] = useState([]);

  useEffect(() => {
    fetch(`${process.env.NEXT_PUBLIC_API_BASE}/api/artists`)
      .then(res => res.json())
      .then(data => setArtists(data));
  }, []);

  return (
    <main className="p-8 max-w-6xl mx-auto">
      <h1 className="text-3xl font-bold mb-8">Artist Growth Radar</h1>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {artists.map((artist: any) => (
          <Link key={artist.artist_id} href={`/artist/${artist.artist_id}`}>
            <div className="border p-6 rounded-lg hover:shadow-lg transition cursor-pointer">
              <h2 className="text-xl font-semibold">{artist.name}</h2>
              <p className="text-gray-500 text-sm">ID: {artist.artist_id}</p>
            </div>
          </Link>
        ))}
      </div>
    </main>
  );
}