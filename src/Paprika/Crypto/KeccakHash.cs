//  Copyright (c) 2021 Demerzel Solutions Limited
//  This file is part of the Nethermind library.
//
//  The Nethermind library is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  The Nethermind library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using static System.Runtime.CompilerServices.Unsafe;
using static System.Numerics.BitOperations;

// ReSharper disable InconsistentNaming
namespace Paprika.Crypto;

public sealed class KeccakHash
{
    private const int ROUNDS = 24;

    private static readonly ulong[] RoundConstants =
    {
        0x0000000000000001UL, 0x0000000000008082UL, 0x800000000000808aUL,
        0x8000000080008000UL, 0x000000000000808bUL, 0x0000000080000001UL,
        0x8000000080008081UL, 0x8000000000008009UL, 0x000000000000008aUL,
        0x0000000000000088UL, 0x0000000080008009UL, 0x000000008000000aUL,
        0x000000008000808bUL, 0x800000000000008bUL, 0x8000000000008089UL,
        0x8000000000008003UL, 0x8000000000008002UL, 0x8000000000000080UL,
        0x000000000000800aUL, 0x800000008000000aUL, 0x8000000080008081UL,
        0x8000000000008080UL, 0x0000000080000001UL, 0x8000000080008008UL
    };

    // update the state with given number of rounds
    private static void KeccakF(ref KeccakBuffer st)
    {
        ulong bCa, bCe, bCi, bCo, bCu;
        ulong da, de, di, @do, du;
        ulong eba, ebe, ebi, ebo, ebu;
        ulong ega, ege, egi, ego, egu;
        ulong eka, eke, eki, eko, eku;
        ulong ema, eme, emi, emo, emu;
        ulong esa, ese, esi, eso, esu;

        for (int round = 0; round < ROUNDS; round += 2)
        {
            //    prepareTheta
            bCa = st.aba() ^ st.aga() ^ st.aka() ^ st.ama() ^ st.asa();
            bCe = st.abe() ^ st.age() ^ st.ake() ^ st.ame() ^ st.ase();
            bCi = st.abi() ^ st.agi() ^ st.aki() ^ st.ami() ^ st.asi();
            bCo = st.abo() ^ st.ago() ^ st.ako() ^ st.amo() ^ st.aso();
            bCu = st.abu() ^ st.agu() ^ st.aku() ^ st.amu() ^ st.asu();

            //thetaRhoPiChiIotaPrepareTheta(round  , A, E)
            da = bCu ^ RotateLeft(bCe, 1);
            de = bCa ^ RotateLeft(bCi, 1);
            di = bCe ^ RotateLeft(bCo, 1);
            @do = bCi ^ RotateLeft(bCu, 1);
            du = bCo ^ RotateLeft(bCa, 1);

            bCa = st.aba() = da ^ st.aba();
            st.age() ^= de;
            bCe = RotateLeft(st.age(), 44);
            st.aki() ^= di;
            bCi = RotateLeft(st.aki(), 43);
            st.amo() ^= @do;
            bCo = RotateLeft(st.amo(), 21);
            st.asu() ^= du;
            bCu = RotateLeft(st.asu(), 14);
            eba = bCa ^ ((~bCe) & bCi);
            eba ^= RoundConstants[round];
            ebe = bCe ^ ((~bCi) & bCo);
            ebi = bCi ^ ((~bCo) & bCu);
            ebo = bCo ^ ((~bCu) & bCa);
            ebu = bCu ^ ((~bCa) & bCe);

            st.abo() ^= @do;
            bCa = RotateLeft(st.abo(), 28);
            st.agu() ^= du;
            bCe = RotateLeft(st.agu(), 20);
            st.aka() ^= da;
            bCi = RotateLeft(st.aka(), 3);
            st.ame() ^= de;
            bCo = RotateLeft(st.ame(), 45);
            st.asi() ^= di;
            bCu = RotateLeft(st.asi(), 61);
            ega = bCa ^ ((~bCe) & bCi);
            ege = bCe ^ ((~bCi) & bCo);
            egi = bCi ^ ((~bCo) & bCu);
            ego = bCo ^ ((~bCu) & bCa);
            egu = bCu ^ ((~bCa) & bCe);

            st.abe() ^= de;
            bCa = RotateLeft(st.abe(), 1);
            st.agi() ^= di;
            bCe = RotateLeft(st.agi(), 6);
            st.ako() ^= @do;
            bCi = RotateLeft(st.ako(), 25);
            st.amu() ^= du;
            bCo = RotateLeft(st.amu(), 8);
            st.asa() ^= da;
            bCu = RotateLeft(st.asa(), 18);
            eka = bCa ^ ((~bCe) & bCi);
            eke = bCe ^ ((~bCi) & bCo);
            eki = bCi ^ ((~bCo) & bCu);
            eko = bCo ^ ((~bCu) & bCa);
            eku = bCu ^ ((~bCa) & bCe);

            st.abu() ^= du;
            bCa = RotateLeft(st.abu(), 27);
            st.aga() ^= da;
            bCe = RotateLeft(st.aga(), 36);
            st.ake() ^= de;
            bCi = RotateLeft(st.ake(), 10);
            st.ami() ^= di;
            bCo = RotateLeft(st.ami(), 15);
            st.aso() ^= @do;
            bCu = RotateLeft(st.aso(), 56);
            ema = bCa ^ ((~bCe) & bCi);
            eme = bCe ^ ((~bCi) & bCo);
            emi = bCi ^ ((~bCo) & bCu);
            emo = bCo ^ ((~bCu) & bCa);
            emu = bCu ^ ((~bCa) & bCe);

            st.abi() ^= di;
            bCa = RotateLeft(st.abi(), 62);
            st.ago() ^= @do;
            bCe = RotateLeft(st.ago(), 55);
            st.aku() ^= du;
            bCi = RotateLeft(st.aku(), 39);
            st.ama() ^= da;
            bCo = RotateLeft(st.ama(), 41);
            st.ase() ^= de;
            bCu = RotateLeft(st.ase(), 2);
            esa = bCa ^ ((~bCe) & bCi);
            ese = bCe ^ ((~bCi) & bCo);
            esi = bCi ^ ((~bCo) & bCu);
            eso = bCo ^ ((~bCu) & bCa);
            esu = bCu ^ ((~bCa) & bCe);

            //    prepareTheta
            bCa = eba ^ ega ^ eka ^ ema ^ esa;
            bCe = ebe ^ ege ^ eke ^ eme ^ ese;
            bCi = ebi ^ egi ^ eki ^ emi ^ esi;
            bCo = ebo ^ ego ^ eko ^ emo ^ eso;
            bCu = ebu ^ egu ^ eku ^ emu ^ esu;

            //thetaRhoPiChiIotaPrepareTheta(round+1, E, A)
            da = bCu ^ RotateLeft(bCe, 1);
            de = bCa ^ RotateLeft(bCi, 1);
            di = bCe ^ RotateLeft(bCo, 1);
            @do = bCi ^ RotateLeft(bCu, 1);
            du = bCo ^ RotateLeft(bCa, 1);

            eba ^= da;
            bCa = eba;
            ege ^= de;
            bCe = RotateLeft(ege, 44);
            eki ^= di;
            bCi = RotateLeft(eki, 43);
            emo ^= @do;
            bCo = RotateLeft(emo, 21);
            esu ^= du;
            bCu = RotateLeft(esu, 14);
            st.aba() = bCa ^ ((~bCe) & bCi);
            st.aba() ^= RoundConstants[round + 1];
            st.abe() = bCe ^ ((~bCi) & bCo);
            st.abi() = bCi ^ ((~bCo) & bCu);
            st.abo() = bCo ^ ((~bCu) & bCa);
            st.abu() = bCu ^ ((~bCa) & bCe);

            ebo ^= @do;
            bCa = RotateLeft(ebo, 28);
            egu ^= du;
            bCe = RotateLeft(egu, 20);
            eka ^= da;
            bCi = RotateLeft(eka, 3);
            eme ^= de;
            bCo = RotateLeft(eme, 45);
            esi ^= di;
            bCu = RotateLeft(esi, 61);
            st.aga() = bCa ^ ((~bCe) & bCi);
            st.age() = bCe ^ ((~bCi) & bCo);
            st.agi() = bCi ^ ((~bCo) & bCu);
            st.ago() = bCo ^ ((~bCu) & bCa);
            st.agu() = bCu ^ ((~bCa) & bCe);

            ebe ^= de;
            bCa = RotateLeft(ebe, 1);
            egi ^= di;
            bCe = RotateLeft(egi, 6);
            eko ^= @do;
            bCi = RotateLeft(eko, 25);
            emu ^= du;
            bCo = RotateLeft(emu, 8);
            esa ^= da;
            bCu = RotateLeft(esa, 18);
            st.aka() = bCa ^ ((~bCe) & bCi);
            st.ake() = bCe ^ ((~bCi) & bCo);
            st.aki() = bCi ^ ((~bCo) & bCu);
            st.ako() = bCo ^ ((~bCu) & bCa);
            st.aku() = bCu ^ ((~bCa) & bCe);

            ebu ^= du;
            bCa = RotateLeft(ebu, 27);
            ega ^= da;
            bCe = RotateLeft(ega, 36);
            eke ^= de;
            bCi = RotateLeft(eke, 10);
            emi ^= di;
            bCo = RotateLeft(emi, 15);
            eso ^= @do;
            bCu = RotateLeft(eso, 56);
            st.ama() = bCa ^ ((~bCe) & bCi);
            st.ame() = bCe ^ ((~bCi) & bCo);
            st.ami() = bCi ^ ((~bCo) & bCu);
            st.amo() = bCo ^ ((~bCu) & bCa);
            st.amu() = bCu ^ ((~bCa) & bCe);

            ebi ^= di;
            bCa = RotateLeft(ebi, 62);
            ego ^= @do;
            bCe = RotateLeft(ego, 55);
            eku ^= du;
            bCi = RotateLeft(eku, 39);
            ema ^= da;
            bCo = RotateLeft(ema, 41);
            ese ^= de;
            bCu = RotateLeft(ese, 2);
            st.asa() = bCa ^ ((~bCe) & bCi);
            st.ase() = bCe ^ ((~bCi) & bCo);
            st.asi() = bCi ^ ((~bCo) & bCu);
            st.aso() = bCo ^ ((~bCu) & bCa);
            st.asu() = bCu ^ ((~bCa) & bCe);
        }
    }

    // compute a Keccak hash (md) of given byte length from "in"
    [SkipLocalsInit]
    public static void ComputeHash(ReadOnlySpan<byte> input, out Keccak output)
    {
        const int roundSize = 136;
        const int ulongRounds = roundSize / sizeof(ulong);

        KeccakBuffer state = default;

        int loopLength = input.Length / roundSize * roundSize;
        if (loopLength > 0)
        {
            ReadOnlySpan<ulong> input64 = MemoryMarshal.Cast<byte, ulong>(input[..loopLength]);
            input = input.Slice(loopLength);
            while (input64.Length > 0)
            {
                for (int i = 0; i < ulongRounds; i++)
                {
                    state[i] ^= input64[i];
                }

                input64 = input64[ulongRounds..];
                KeccakF(ref state);
            }
        }

        Span<byte> temp = stackalloc byte[roundSize];
        temp.Clear();
        input.CopyTo(temp);
        temp[input.Length] = 1;
        temp[roundSize - 1] |= 0x80;

        Span<ulong> tempU64 = MemoryMarshal.Cast<byte, ulong>(temp);
        for (int i = 0; i < tempU64.Length; i++)
        {
            state[i] ^= tempU64[i];
        }

        KeccakF(ref state);
        output = As<KeccakBuffer, Keccak>(ref state);
    }

    [DoesNotReturn]
    private static void ThrowBadKeccak() => throw new ArgumentException("Bad Keccak use");
}

[InlineArray(KeccakBufferCount)]
public struct KeccakBuffer
{
    public const int KeccakBufferCount = 25;
    private ulong st;
}

public static class BufferExtensions
{
    public static ref ulong aba(ref this KeccakBuffer buffer) => ref buffer[0];
    public static ref ulong abe(ref this KeccakBuffer buffer) => ref buffer[1];
    public static ref ulong abi(ref this KeccakBuffer buffer) => ref buffer[2];
    public static ref ulong abo(ref this KeccakBuffer buffer) => ref buffer[3];
    public static ref ulong abu(ref this KeccakBuffer buffer) => ref buffer[4];
    public static ref ulong aga(ref this KeccakBuffer buffer) => ref buffer[5];
    public static ref ulong age(ref this KeccakBuffer buffer) => ref buffer[6];
    public static ref ulong agi(ref this KeccakBuffer buffer) => ref buffer[7];
    public static ref ulong ago(ref this KeccakBuffer buffer) => ref buffer[8];
    public static ref ulong agu(ref this KeccakBuffer buffer) => ref buffer[9];
    public static ref ulong aka(ref this KeccakBuffer buffer) => ref buffer[10];
    public static ref ulong ake(ref this KeccakBuffer buffer) => ref buffer[11];
    public static ref ulong aki(ref this KeccakBuffer buffer) => ref buffer[12];
    public static ref ulong ako(ref this KeccakBuffer buffer) => ref buffer[13];
    public static ref ulong aku(ref this KeccakBuffer buffer) => ref buffer[14];
    public static ref ulong ama(ref this KeccakBuffer buffer) => ref buffer[15];
    public static ref ulong ame(ref this KeccakBuffer buffer) => ref buffer[16];
    public static ref ulong ami(ref this KeccakBuffer buffer) => ref buffer[17];
    public static ref ulong amo(ref this KeccakBuffer buffer) => ref buffer[18];
    public static ref ulong amu(ref this KeccakBuffer buffer) => ref buffer[19];
    public static ref ulong asa(ref this KeccakBuffer buffer) => ref buffer[20];
    public static ref ulong ase(ref this KeccakBuffer buffer) => ref buffer[21];
    public static ref ulong asi(ref this KeccakBuffer buffer) => ref buffer[22];
    public static ref ulong aso(ref this KeccakBuffer buffer) => ref buffer[23];
    public static ref ulong asu(ref this KeccakBuffer buffer) => ref buffer[24];
}
